/*
Copyright 2022 The Photon Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <gtest/gtest.h>
#include <photon/alilfs-extend/shared_mutex.h>
#include <photon/common/alog.h>
#include <photon/photon.h>
#include <photon/thread/thread.h>
#include <photon/thread/thread11.h>

#include <atomic>
#include <thread>
#include <vector>

// =============================================================================
// Basic Functionality Tests
// =============================================================================

TEST(SharedMutex, BasicLockUnlock) {
    photon_lfsextend::shared_mutex mtx;

    // Test exclusive lock
    EXPECT_EQ(0, mtx.lock());
    mtx.unlock();

    // Test shared lock
    EXPECT_EQ(0, mtx.lock_shared());
    mtx.unlock_shared();
}

TEST(SharedMutex, TryLock) {
    photon_lfsextend::shared_mutex mtx;

    // Try lock should succeed when unlocked
    EXPECT_TRUE(mtx.trylock());
    // Try lock should fail when write-locked
    EXPECT_FALSE(mtx.trylock());
    EXPECT_FALSE(mtx.trylock_shared());
    mtx.unlock();

    // Try shared lock should succeed when unlocked
    EXPECT_TRUE(mtx.trylock_shared());
    // Additional shared locks should succeed
    EXPECT_TRUE(mtx.trylock_shared());
    // Write lock should fail when read-locked
    EXPECT_FALSE(mtx.trylock());
    mtx.unlock_shared();
    mtx.unlock_shared();
}

TEST(SharedMutex, MultipleReaders) {
    photon_lfsextend::shared_mutex mtx;
    constexpr int NUM_READERS = 100;

    // Acquire many shared locks
    for (int i = 0; i < NUM_READERS; i++) {
        EXPECT_TRUE(mtx.trylock_shared());
    }

    // Write lock should fail
    EXPECT_FALSE(mtx.trylock());

    // Release all shared locks
    for (int i = 0; i < NUM_READERS; i++) {
        mtx.unlock_shared();
    }

    // Now write lock should succeed
    EXPECT_TRUE(mtx.trylock());
    mtx.unlock();
}

// =============================================================================
// Concurrent Correctness Tests
// =============================================================================

TEST(SharedMutex, ConcurrentReaders) {
    photon_lfsextend::shared_mutex mtx;
    std::atomic<int> active_readers{0};
    std::atomic<int> max_concurrent{0};
    std::atomic<uint64_t> total_ops{0};
    constexpr int NUM_THREADS = 32;
    constexpr int OPS_PER_THREAD = 1000;

    std::vector<photon::join_handle*> handles;
    for (int i = 0; i < NUM_THREADS; i++) {
        handles.emplace_back(photon::thread_enable_join(
            photon::thread_create11([&]() {
                for (int j = 0; j < OPS_PER_THREAD; j++) {
                    mtx.lock_shared();
                    int cur = active_readers.fetch_add(1, std::memory_order_relaxed) + 1;
                    // Track max concurrent readers
                    int prev_max = max_concurrent.load(std::memory_order_relaxed);
                    while (cur > prev_max &&
                           !max_concurrent.compare_exchange_weak(prev_max, cur)) {}
                    photon::thread_yield();
                    active_readers.fetch_sub(1, std::memory_order_relaxed);
                    mtx.unlock_shared();
                    total_ops.fetch_add(1, std::memory_order_relaxed);
                }
            })));
    }

    for (auto& h : handles) {
        photon::thread_join(h);
    }

    EXPECT_EQ(0, active_readers.load());
    EXPECT_GT(max_concurrent.load(), 1);  // Should have concurrent readers
    EXPECT_EQ(NUM_THREADS * OPS_PER_THREAD, total_ops.load());
    LOG_INFO("Max concurrent readers: `", max_concurrent.load());
}

TEST(SharedMutex, ReaderWriterExclusion) {
    photon_lfsextend::shared_mutex mtx;
    std::atomic<bool> writing{false};
    std::atomic<int> active_readers{0};
    std::atomic<uint64_t> read_ops{0};
    std::atomic<uint64_t> write_ops{0};
    constexpr int NUM_READERS = 16;
    constexpr int NUM_WRITERS = 4;
    constexpr int OPS_PER_THREAD = 500;

    std::vector<photon::join_handle*> handles;

    // Reader threads
    for (int i = 0; i < NUM_READERS; i++) {
        handles.emplace_back(photon::thread_enable_join(
            photon::thread_create11([&]() {
                for (int j = 0; j < OPS_PER_THREAD; j++) {
                    mtx.lock_shared();
                    active_readers.fetch_add(1, std::memory_order_relaxed);
                    EXPECT_FALSE(writing.load(std::memory_order_acquire));
                    photon::thread_yield();
                    EXPECT_FALSE(writing.load(std::memory_order_acquire));
                    active_readers.fetch_sub(1, std::memory_order_relaxed);
                    mtx.unlock_shared();
                    read_ops.fetch_add(1, std::memory_order_relaxed);
                }
            })));
    }

    // Writer threads
    for (int i = 0; i < NUM_WRITERS; i++) {
        handles.emplace_back(photon::thread_enable_join(
            photon::thread_create11([&]() {
                for (int j = 0; j < OPS_PER_THREAD; j++) {
                    mtx.lock();
                    EXPECT_EQ(0, active_readers.load(std::memory_order_acquire));
                    writing.store(true, std::memory_order_release);
                    photon::thread_yield();
                    EXPECT_EQ(0, active_readers.load(std::memory_order_acquire));
                    writing.store(false, std::memory_order_release);
                    mtx.unlock();
                    write_ops.fetch_add(1, std::memory_order_relaxed);
                }
            })));
    }

    for (auto& h : handles) {
        photon::thread_join(h);
    }

    EXPECT_EQ(NUM_READERS * OPS_PER_THREAD, read_ops.load());
    EXPECT_EQ(NUM_WRITERS * OPS_PER_THREAD, write_ops.load());
    LOG_INFO("Read ops: `, Write ops: `", read_ops.load(), write_ops.load());
}

// =============================================================================
// Timeout Tests
// =============================================================================

TEST(SharedMutex, LockTimeout) {
    photon_lfsextend::shared_mutex mtx;

    // Acquire write lock in main coroutine
    EXPECT_EQ(0, mtx.lock());

    // Try to acquire read lock with timeout in another coroutine
    std::atomic<int> result{0};
    std::atomic<uint64_t> elapsed{0};

    auto jh = photon::thread_enable_join(
        photon::thread_create11([&]() {
            auto start = photon::now;
            result.store(mtx.lock_shared(100 * 1000), std::memory_order_relaxed);
            elapsed.store(photon::now - start, std::memory_order_relaxed);
        }));

    // Must yield to let the waiter run and timeout
    photon::thread_usleep(200 * 1000);  // Sleep longer than timeout

    photon::thread_join(jh);
    mtx.unlock();

    EXPECT_EQ(-ETIMEDOUT, result.load());
    EXPECT_GE(elapsed.load(), 100 * 1000UL);
}

TEST(SharedMutex, WriteLockTimeout) {
    photon_lfsextend::shared_mutex mtx;

    // Acquire read lock in main coroutine
    EXPECT_EQ(0, mtx.lock_shared());

    // Try to acquire write lock with timeout in another coroutine
    std::atomic<int> result{0};
    std::atomic<uint64_t> elapsed{0};

    auto jh = photon::thread_enable_join(
        photon::thread_create11([&]() {
            auto start = photon::now;
            result.store(mtx.lock(100 * 1000), std::memory_order_relaxed);
            elapsed.store(photon::now - start, std::memory_order_relaxed);
        }));

    // Must yield to let the waiter run and timeout
    photon::thread_usleep(200 * 1000);  // Sleep longer than timeout

    photon::thread_join(jh);
    mtx.unlock_shared();

    EXPECT_EQ(-ETIMEDOUT, result.load());
    EXPECT_GE(elapsed.load(), 100 * 1000UL);
}

// =============================================================================
// Multi-vCPU Tests
// =============================================================================

TEST(SharedMutex, MultiVcpuConcurrency) {
    photon_lfsextend::shared_mutex mtx;
    std::atomic<int64_t> counter{0};
    std::atomic<uint64_t> total_ops{0};
    constexpr int NUM_VCPUS = 4;
    constexpr int THREADS_PER_VCPU = 16;
    constexpr int OPS_PER_THREAD = 500;

    std::vector<std::thread> vcpu_threads;
    for (int v = 0; v < NUM_VCPUS; v++) {
        vcpu_threads.emplace_back([&]() {
            photon::vcpu_init();
            DEFER(photon::vcpu_fini());

            std::vector<photon::join_handle*> handles;
            for (int i = 0; i < THREADS_PER_VCPU; i++) {
                handles.emplace_back(photon::thread_enable_join(
                    photon::thread_create11([&]() {
                        for (int j = 0; j < OPS_PER_THREAD; j++) {
                            if (rand() % 10 < 7) {  // 70% read
                                mtx.lock_shared();
                                (void)counter.load(std::memory_order_acquire);
                                mtx.unlock_shared();
                            } else {  // 30% write
                                mtx.lock();
                                counter.fetch_add(1, std::memory_order_relaxed);
                                mtx.unlock();
                            }
                            total_ops.fetch_add(1, std::memory_order_relaxed);
                        }
                    })));
            }

            for (auto& h : handles) {
                photon::thread_join(h);
            }
        });
    }

    for (auto& t : vcpu_threads) {
        t.join();
    }

    EXPECT_EQ(NUM_VCPUS * THREADS_PER_VCPU * OPS_PER_THREAD, total_ops.load());
    LOG_INFO("Total ops: `, Counter: `", total_ops.load(), counter.load());
}

// =============================================================================
// std-compat Interface Tests
// =============================================================================

TEST(StdSharedMutex, BasicUsage) {
    photon_std::shared_mutex mtx;

    mtx.lock();
    mtx.unlock();

    mtx.lock_shared();
    mtx.lock_shared();
    mtx.unlock_shared();
    mtx.unlock_shared();
}

TEST(StdSharedTimedMutex, TimedLock) {
    photon_std::shared_timed_mutex mtx;

    // Acquire lock in main coroutine
    mtx.lock();

    // Try lock with timeout in another coroutine
    std::atomic<bool> lock_failed{false};
    std::atomic<bool> shared_lock_failed{false};

    auto jh = photon::thread_enable_join(
        photon::thread_create11([&]() {
            lock_failed.store(!mtx.try_lock_for(std::chrono::milliseconds(50)),
                             std::memory_order_relaxed);
            shared_lock_failed.store(!mtx.try_lock_shared_for(std::chrono::milliseconds(50)),
                                     std::memory_order_relaxed);
        }));

    // Must yield to let the waiter run and timeout
    photon::thread_usleep(150 * 1000);  // Sleep longer than timeout

    photon::thread_join(jh);
    mtx.unlock();

    EXPECT_TRUE(lock_failed.load());
    EXPECT_TRUE(shared_lock_failed.load());

    // Now should succeed
    EXPECT_TRUE(mtx.try_lock_for(std::chrono::milliseconds(50)));
    mtx.unlock();

    EXPECT_TRUE(mtx.try_lock_shared_for(std::chrono::milliseconds(50)));
    mtx.unlock_shared();
}

// =============================================================================
// Multi-thread (std::thread) Tests
// These tests verify correctness when the lock is accessed from multiple
// OS threads, each running their own photon vCPU.
// =============================================================================

TEST(MultiThread, ConcurrentReadersAcrossThreads) {
    photon_lfsextend::shared_mutex mtx;
    std::atomic<int> active_readers{0};
    std::atomic<int> max_concurrent{0};
    std::atomic<uint64_t> total_ops{0};
    std::atomic<bool> writer_seen{false};
    constexpr int NUM_THREADS = 4;
    constexpr int COROUTINES_PER_THREAD = 16;
    constexpr int OPS_PER_COROUTINE = 200;

    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&]() {
            photon::vcpu_init();
            DEFER(photon::vcpu_fini());

            std::vector<photon::join_handle*> handles;
            for (int i = 0; i < COROUTINES_PER_THREAD; i++) {
                handles.emplace_back(photon::thread_enable_join(
                    photon::thread_create11([&]() {
                        for (int j = 0; j < OPS_PER_COROUTINE; j++) {
                            mtx.lock_shared();
                            int cur = active_readers.fetch_add(1, std::memory_order_relaxed) + 1;
                            // Track max concurrent readers
                            int prev_max = max_concurrent.load(std::memory_order_relaxed);
                            while (cur > prev_max &&
                                   !max_concurrent.compare_exchange_weak(prev_max, cur)) {}
                            // Verify no writer is active
                            EXPECT_FALSE(writer_seen.load(std::memory_order_acquire));
                            photon::thread_yield();
                            active_readers.fetch_sub(1, std::memory_order_relaxed);
                            mtx.unlock_shared();
                            total_ops.fetch_add(1, std::memory_order_relaxed);
                        }
                    })));
            }

            for (auto& h : handles) {
                photon::thread_join(h);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(0, active_readers.load());
    EXPECT_GT(max_concurrent.load(), 1);  // Should have concurrent readers
    EXPECT_EQ(NUM_THREADS * COROUTINES_PER_THREAD * OPS_PER_COROUTINE, total_ops.load());
    LOG_INFO("Multi-thread concurrent readers test: max_concurrent=`, total_ops=`",
             max_concurrent.load(), total_ops.load());
}

TEST(MultiThread, ReadWriteExclusionAcrossThreads) {
    photon_lfsextend::shared_mutex mtx;
    std::atomic<bool> writing{false};
    std::atomic<int> active_readers{0};
    std::atomic<uint64_t> read_ops{0};
    std::atomic<uint64_t> write_ops{0};
    constexpr int NUM_THREADS = 4;
    constexpr int COROUTINES_PER_THREAD = 16;
    constexpr int OPS_PER_COROUTINE = 100;

    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&, t]() {
            photon::vcpu_init();
            DEFER(photon::vcpu_fini());

            std::vector<photon::join_handle*> handles;
            for (int i = 0; i < COROUTINES_PER_THREAD; i++) {
                bool is_writer = (t == 0 && i < 4);  // Only first 4 coroutines in thread 0 are writers
                handles.emplace_back(photon::thread_enable_join(
                    photon::thread_create11([&, is_writer]() {
                        for (int j = 0; j < OPS_PER_COROUTINE; j++) {
                            if (is_writer) {
                                mtx.lock();
                                EXPECT_EQ(0, active_readers.load(std::memory_order_acquire));
                                EXPECT_FALSE(writing.load(std::memory_order_acquire));
                                writing.store(true, std::memory_order_release);
                                photon::thread_yield();
                                EXPECT_EQ(0, active_readers.load(std::memory_order_acquire));
                                writing.store(false, std::memory_order_release);
                                mtx.unlock();
                                write_ops.fetch_add(1, std::memory_order_relaxed);
                            } else {
                                mtx.lock_shared();
                                active_readers.fetch_add(1, std::memory_order_relaxed);
                                EXPECT_FALSE(writing.load(std::memory_order_acquire));
                                photon::thread_yield();
                                EXPECT_FALSE(writing.load(std::memory_order_acquire));
                                active_readers.fetch_sub(1, std::memory_order_relaxed);
                                mtx.unlock_shared();
                                read_ops.fetch_add(1, std::memory_order_relaxed);
                            }
                        }
                    })));
            }

            for (auto& h : handles) {
                photon::thread_join(h);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    uint64_t expected_writes = 4 * OPS_PER_COROUTINE;  // 4 writers
    uint64_t expected_reads = (NUM_THREADS * COROUTINES_PER_THREAD - 4) * OPS_PER_COROUTINE;
    EXPECT_EQ(expected_writes, write_ops.load());
    EXPECT_EQ(expected_reads, read_ops.load());
    LOG_INFO("Multi-thread read/write exclusion test: read_ops=`, write_ops=`",
             read_ops.load(), write_ops.load());
}

TEST(MultiThread, StressTest) {
    photon_lfsextend::shared_mutex mtx;
    std::atomic<int64_t> counter{0};
    std::atomic<uint64_t> total_ops{0};
    constexpr int NUM_THREADS = 8;
    constexpr int COROUTINES_PER_THREAD = 32;
    constexpr int OPS_PER_COROUTINE = 500;

    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&]() {
            photon::vcpu_init();
            DEFER(photon::vcpu_fini());
            unsigned int seed = (unsigned int)(std::hash<std::thread::id>{}(
                std::this_thread::get_id()) ^ photon::now);

            std::vector<photon::join_handle*> handles;
            for (int i = 0; i < COROUTINES_PER_THREAD; i++) {
                handles.emplace_back(photon::thread_enable_join(
                    photon::thread_create11([&, seed]() mutable {
                        for (int j = 0; j < OPS_PER_COROUTINE; j++) {
                            if (rand_r(&seed) % 10 < 7) {  // 70% read
                                mtx.lock_shared();
                                (void)counter.load(std::memory_order_acquire);
                                mtx.unlock_shared();
                            } else {  // 30% write
                                mtx.lock();
                                counter.fetch_add(1, std::memory_order_relaxed);
                                mtx.unlock();
                            }
                            total_ops.fetch_add(1, std::memory_order_relaxed);
                        }
                    })));
            }

            for (auto& h : handles) {
                photon::thread_join(h);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(NUM_THREADS * COROUTINES_PER_THREAD * OPS_PER_COROUTINE, total_ops.load());
    LOG_INFO("Multi-thread stress test: total_ops=`, counter=`",
             total_ops.load(), counter.load());
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    photon::init(photon::INIT_EVENT_DEFAULT, photon::INIT_IO_NONE);
    DEFER(photon::fini());

    set_log_output_level(ALOG_INFO);

    return RUN_ALL_TESTS();
}
