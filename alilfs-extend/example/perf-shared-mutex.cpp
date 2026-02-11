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

// Performance benchmark comparing photon_lfsextend::shared_mutex vs photon::rwlock
//
// Tests different read/write ratios:
// - 100% read (pure read workload)
// - 90% read / 10% write (read-heavy)
// - 50% read / 50% write (balanced)

#include <photon/alilfs-extend/shared_mutex.h>
#include <photon/common/alog.h>
#include <photon/photon.h>
#include <photon/thread/thread.h>
#include <photon/thread/thread11.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

// =============================================================================
// Benchmark Configuration
// =============================================================================

struct BenchConfig {
    int num_vcpus = 4;
    int threads_per_vcpu = 32;
    int duration_seconds = 5;
    int read_percent = 90;  // percentage of read operations
    const char* name = "default";
};

// =============================================================================
// Lock Wrapper Interfaces
// =============================================================================

// Wrapper for photon_lfsextend::shared_mutex
class LfsextendLockWrapper {
public:
    void lock_shared() { mtx.lock_shared(); }
    void unlock_shared() { mtx.unlock_shared(); }
    void lock() { mtx.lock(); }
    void unlock() { mtx.unlock(); }
    static const char* name() { return "photon_lfsextend::shared_mutex"; }
private:
    photon_lfsextend::shared_mutex mtx;
};

// Wrapper for photon::rwlock
class PhotonRwlockWrapper {
public:
    void lock_shared() { rwl.lock(photon::RLOCK); }
    void unlock_shared() { rwl.unlock(); }
    void lock() { rwl.lock(photon::WLOCK); }
    void unlock() { rwl.unlock(); }
    static const char* name() { return "photon::rwlock"; }
private:
    photon::rwlock rwl;
};

// =============================================================================
// Benchmark Runner
// =============================================================================

template <typename LockType>
struct BenchResult {
    uint64_t total_ops = 0;
    uint64_t read_ops = 0;
    uint64_t write_ops = 0;
    double duration_ms = 0;
    double qps = 0;
    double avg_latency_ns = 0;
};

template <typename LockType>
BenchResult<LockType> run_benchmark(const BenchConfig& config) {
    LockType lock;
    std::atomic<bool> running{true};
    std::atomic<uint64_t> total_read_ops{0};
    std::atomic<uint64_t> total_write_ops{0};
    std::atomic<uint64_t> total_latency_ns{0};

    // Shared data protected by the lock
    std::atomic<int64_t> counter{0};

    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> vcpu_threads;
    for (int v = 0; v < config.num_vcpus; v++) {
        vcpu_threads.emplace_back([&]() {
            photon::vcpu_init();
            DEFER(photon::vcpu_fini());

            std::vector<photon::join_handle*> handles;
            for (int i = 0; i < config.threads_per_vcpu; i++) {
                handles.emplace_back(photon::thread_enable_join(
                    photon::thread_create11([&]() {
                        uint64_t local_read_ops = 0;
                        uint64_t local_write_ops = 0;
                        uint64_t local_latency_ns = 0;
                        // Use thread-local random seed
                        unsigned int seed = (unsigned int)(photon::now ^ (uint64_t)photon::CURRENT);

                        while (running.load(std::memory_order_relaxed)) {
                            bool is_read = (rand_r(&seed) % 100) < config.read_percent;

                            auto op_start = std::chrono::high_resolution_clock::now();

                            if (is_read) {
                                lock.lock_shared();
                                // Simulate read operation
                                (void)counter.load(std::memory_order_acquire);
                                lock.unlock_shared();
                                local_read_ops++;
                            } else {
                                lock.lock();
                                // Simulate write operation
                                counter.fetch_add(1, std::memory_order_relaxed);
                                lock.unlock();
                                local_write_ops++;
                            }

                            auto op_end = std::chrono::high_resolution_clock::now();
                            local_latency_ns += std::chrono::duration_cast<
                                std::chrono::nanoseconds>(op_end - op_start).count();

                            // Yield occasionally to allow other coroutines to run
                            if ((local_read_ops + local_write_ops) % 100 == 0) {
                                photon::thread_yield();
                            }
                        }

                        total_read_ops.fetch_add(local_read_ops, std::memory_order_relaxed);
                        total_write_ops.fetch_add(local_write_ops, std::memory_order_relaxed);
                        total_latency_ns.fetch_add(local_latency_ns, std::memory_order_relaxed);
                    })));
            }

            // Wait for duration
            photon::thread_sleep(config.duration_seconds);
            running.store(false, std::memory_order_release);

            for (auto& h : handles) {
                photon::thread_join(h);
            }
        });
    }

    for (auto& t : vcpu_threads) {
        t.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();

    BenchResult<LockType> result;
    result.read_ops = total_read_ops.load();
    result.write_ops = total_write_ops.load();
    result.total_ops = result.read_ops + result.write_ops;
    result.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time).count();
    result.qps = result.total_ops / (result.duration_ms / 1000.0);
    result.avg_latency_ns = result.total_ops > 0 ?
        (double)total_latency_ns.load() / result.total_ops : 0;

    return result;
}

// =============================================================================
// Benchmark Scenarios
// =============================================================================

void print_separator() {
    printf("========================================================================\n");
}

void print_result_header() {
    printf("%-35s %10s %12s %12s   %s\n", "Lock Type", "Total Ops", "QPS", "Avg Lat(ns)", "Read/Write");
}

template <typename LockType>
void print_result(const BenchResult<LockType>& result) {
    printf("%-35s %10lu %12.0f %12.0f   %lu/%lu\n",
           LockType::name(), result.total_ops, result.qps,
           result.avg_latency_ns, result.read_ops, result.write_ops);
}

void run_scenario(const char* scenario_name, int read_percent,
                  int num_vcpus, int threads_per_vcpu, int duration_seconds) {
    print_separator();
    printf("Scenario: %s (Read: %d%%, Write: %d%%)\n",
           scenario_name, read_percent, 100 - read_percent);
    printf("Config: %d vCPUs, %d threads/vCPU, %d seconds\n",
           num_vcpus, threads_per_vcpu, duration_seconds);
    print_separator();
    print_result_header();

    BenchConfig config;
    config.num_vcpus = num_vcpus;
    config.threads_per_vcpu = threads_per_vcpu;
    config.duration_seconds = duration_seconds;
    config.read_percent = read_percent;

    // Run benchmark for photon_lfsextend::shared_mutex
    auto result1 = run_benchmark<LfsextendLockWrapper>(config);
    print_result(result1);

    // Run benchmark for photon::rwlock
    auto result2 = run_benchmark<PhotonRwlockWrapper>(config);
    print_result(result2);

    // Print comparison
    printf("\n");
    double speedup = result1.qps / result2.qps;
    if (speedup >= 1.0) {
        printf("Performance comparison: photon_lfsextend::shared_mutex is %.2fx faster than photon::rwlock\n", speedup);
    } else {
        printf("Performance comparison: photon_lfsextend::shared_mutex is %.2fx slower than photon::rwlock\n", 1.0 / speedup);
    }
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char** argv) {
    // Initialize photon for the main thread first
    photon::init(photon::INIT_EVENT_DEFAULT, photon::INIT_IO_NONE);
    DEFER(photon::fini());

    printf("\n");
    printf("======================================================================\n");
    printf("       shared_mutex vs rwlock Performance Benchmark\n");
    printf("======================================================================\n");
    printf("\n");

    // Configuration
    int num_vcpus = 4;
    int threads_per_vcpu = 32;
    int duration_seconds = 3;

    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--vcpus") == 0 && i + 1 < argc) {
            num_vcpus = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            threads_per_vcpu = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--duration") == 0 && i + 1 < argc) {
            duration_seconds = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0) {
            printf("Usage: %s [options]\n", argv[0]);
            printf("Options:\n");
            printf("  --vcpus N      Number of vCPUs (default: 4)\n");
            printf("  --threads N    Threads per vCPU (default: 32)\n");
            printf("  --duration N   Test duration in seconds (default: 3)\n");
            return 0;
        }
    }

    printf("Running with: %d vCPUs, %d threads/vCPU, %d seconds per scenario\n",
           num_vcpus, threads_per_vcpu, duration_seconds);
    printf("\n");

    // Scenario 1: Pure Read (100% read)
    run_scenario("Pure Read Workload", 100,
                 num_vcpus, threads_per_vcpu, duration_seconds);

    // Scenario 2: Read-Heavy (90% read, 10% write)
    run_scenario("Read-Heavy Workload", 90,
                 num_vcpus, threads_per_vcpu, duration_seconds);

    // Scenario 3: Moderate Read (70% read, 30% write)
    run_scenario("Moderate Read Workload", 70,
                 num_vcpus, threads_per_vcpu, duration_seconds);

    // Scenario 4: Balanced (50% read, 50% write)
    run_scenario("Balanced Workload", 50,
                 num_vcpus, threads_per_vcpu, duration_seconds);

    printf("\n");
    print_separator();
    printf("Benchmark completed.\n");
    print_separator();

    return 0;
}
