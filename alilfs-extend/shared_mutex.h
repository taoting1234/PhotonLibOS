#pragma once
#include <photon/common/timeout.h>
#include <photon/thread/std-compat.h>
#include <photon/thread/thread.h>

#include <atomic>
#include <chrono>

namespace photon_lfsextend {

// High-performance shared_mutex without starvation prevention.
// Optimized for read-heavy workloads using lock-free atomic operations
// for the read lock fast path.
//
// State encoding:
//   state > 0  : number of active readers
//   state == 0 : unlocked
//   state == -1: write-locked
class shared_mutex {
protected:
    constexpr static int64_t MAX_SHARED_LOCK_COUNT = 1 << 16;
    constexpr static int64_t WRITE_LOCKED = -1;

    std::atomic<int64_t> lock_state{0};
    photon::condition_variable cv_shared;
    photon::condition_variable cv_unique;
    photon::spinlock spin;

    // Try to wake up waiting threads after unlock.
    // Must be called with spin lock held.
    void try_wake() {
        // Prefer waking writers first, then all readers
        if (!cv_unique.notify_one()) {
            cv_shared.notify_all();
        }
    }

public:
    shared_mutex() = default;
    shared_mutex(const shared_mutex&) = delete;
    shared_mutex& operator=(const shared_mutex&) = delete;

    // Try to acquire exclusive (write) lock without blocking.
    // Returns true on success, false if lock is held.
    bool trylock() {
        int64_t expected = 0;
        return lock_state.compare_exchange_strong(
            expected, WRITE_LOCKED,
            std::memory_order_acq_rel, std::memory_order_relaxed);
    }

    // Try to acquire shared (read) lock without blocking.
    // Lock-free fast path using CAS - no spinlock needed.
    // Returns true on success, false if write-locked or max readers reached.
    bool trylock_shared() {
        auto state = lock_state.load(std::memory_order_acquire);
        while (state >= 0 && state < MAX_SHARED_LOCK_COUNT) {
            if (lock_state.compare_exchange_weak(
                    state, state + 1,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                return true;
            }
            // state is updated by compare_exchange_weak on failure
        }
        return false;
    }

    // Acquire exclusive (write) lock, blocking until available.
    // Returns 0 on success, -ETIMEDOUT on timeout.
    int lock(uint64_t timeout = -1) {
        // Fast path: try lock-free acquisition first
        if (trylock()) {
            return 0;
        }

        // Slow path: wait with spinlock protection
        photon::Timeout tmo(timeout);
        SCOPED_LOCK(spin);
        while (true) {
            int64_t expected = 0;
            if (lock_state.compare_exchange_strong(
                    expected, WRITE_LOCKED,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                return 0;
            }
            if (cv_unique.wait(spin, tmo.timeout()) < 0) {
                return -errno;  // ETIMEDOUT or other errors
            }
        }
    }

    // Acquire shared (read) lock, blocking until available.
    // Returns 0 on success, -ETIMEDOUT on timeout.
    int lock_shared(uint64_t timeout = -1) {
        // Fast path: lock-free acquisition (common case)
        if (trylock_shared()) {
            return 0;
        }

        // Slow path: wait with spinlock protection
        photon::Timeout tmo(timeout);
        SCOPED_LOCK(spin);
        while (true) {
            auto state = lock_state.load(std::memory_order_acquire);
            while (state >= 0 && state < MAX_SHARED_LOCK_COUNT) {
                if (lock_state.compare_exchange_weak(
                        state, state + 1,
                        std::memory_order_acq_rel, std::memory_order_acquire)) {
                    return 0;
                }
            }
            if (cv_shared.wait(spin, tmo.timeout()) < 0) {
                return -errno;  // ETIMEDOUT or other errors
            }
        }
    }

    // Release exclusive (write) lock.
    // WARNING: Caller must ensure they actually hold the write lock.
    // Calling unlock() without holding the write lock is undefined behavior.
    void unlock() {
        lock_state.store(0, std::memory_order_release);
        // Wake up waiting threads
        SCOPED_LOCK(spin);
        try_wake();
    }

    // Release shared (read) lock.
    // Lock-free using atomic decrement.
    // WARNING: Caller must ensure they actually hold a read lock.
    // Calling unlock_shared() without holding a read lock is undefined behavior.
    void unlock_shared() {
        auto prev = lock_state.fetch_sub(1, std::memory_order_acq_rel);
        // If this was the last reader, wake up waiting writers
        // CRITICAL: Must unconditionally wake without double-checking state.
        // Race scenario: after fetch_sub but before acquiring spinlock,
        // another thread may acquire shared lock via trylock_shared().
        // If we check state again and see readers, we won't wake waiters,
        // causing writer deadlock since the new reader used lock-free path.
        if (prev == 1) {
            SCOPED_LOCK(spin);
            try_wake();
        }
    }
};

}  // namespace photon_lfsextend

namespace photon_std {

class shared_timed_mutex {
    photon_lfsextend::shared_mutex smtx;

public:
    shared_timed_mutex() {}
    shared_timed_mutex(const shared_timed_mutex&) = delete;
    shared_timed_mutex operator=(const shared_timed_mutex&) = delete;

    void lock() {
        auto ret = smtx.lock();
        if (ret < 0) __throw_system_error(ret, "lock failed");
    }
    bool try_lock() { return smtx.trylock(); }
    template <class Rep, class Period>
    bool try_lock_for(
        const std::chrono::duration<Rep, Period>& timeout_duration) {
        return smtx.lock(__duration_to_microseconds(timeout_duration)) == 0;
    }
    template <class Clock, class Duration>
    bool try_lock_until(
        const std::chrono::time_point<Clock, Duration>& timeout_time) {
        return smtx.lock(__duration_to_microseconds(
                   timeout_time - ::std::chrono::steady_clock::now())) == 0;
    }
    void unlock() { smtx.unlock(); }
    void lock_shared() {
        auto ret = smtx.lock_shared();
        if (ret < 0) __throw_system_error(ret, "lock failed");
    }
    bool try_lock_shared() { return smtx.trylock_shared(); }
    template <class Rep, class Period>
    bool try_lock_shared_for(
        const std::chrono::duration<Rep, Period>& timeout_duration) {
        return smtx.lock_shared(__duration_to_microseconds(timeout_duration)) ==
               0;
    }
    template <class Clock, class Duration>
    bool try_lock_shared_until(
        const std::chrono::time_point<Clock, Duration>& timeout_time) {
        return smtx.lock_shared(__duration_to_microseconds(
                   timeout_time - ::std::chrono::steady_clock::now())) == 0;
    }
    void unlock_shared() { smtx.unlock_shared(); }
};

class shared_mutex {
    photon_lfsextend::shared_mutex smtx;

public:
    shared_mutex() {}
    shared_mutex(const shared_mutex&) = delete;
    shared_mutex operator=(const shared_mutex&) = delete;

    using native_handle_type = photon_lfsextend::shared_mutex&;
    native_handle_type native_handle() { return smtx; }
    void lock() {
        auto ret = smtx.lock();
        if (ret < 0) __throw_system_error(ret, "lock failed");
    }
    bool try_lock() { return smtx.trylock(); }
    void unlock() { smtx.unlock(); }
    void lock_shared() {
        auto ret = smtx.lock_shared();
        if (ret < 0) __throw_system_error(ret, "lock failed");
    }
    bool try_lock_shared() { return smtx.trylock_shared(); }
    void unlock_shared() { smtx.unlock_shared(); }
};

}  // namespace photon_std