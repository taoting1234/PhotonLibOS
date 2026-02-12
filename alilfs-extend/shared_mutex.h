#pragma once
#include <photon/common/timeout.h>
#include <photon/thread/std-compat.h>
#include <photon/thread/thread.h>

#include <atomic>
#include <cassert>
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
    photon::thread* write_lock_owner{nullptr};

    // Must be called with spinlock held.
    void try_wake() {
        if (!cv_unique.notify_one()) {
            cv_shared.notify_all();
        }
    }

    // Common blocking lock pattern: fast-path try, then slow-path wait loop.
    // try_fn must be safe to call both with and without spinlock held.
    template<typename TryFunc>
    int do_lock(TryFunc&& try_fn, photon::condition_variable& cv, uint64_t timeout) {
        if (try_fn()) return 0;
        photon::Timeout tmo(timeout);
        SCOPED_LOCK(spin);
        while (true) {
            if (try_fn()) return 0;
            int ret = cv.wait(spin, tmo.timeout());
            if (ret < 0) {
                int err = errno;
                if (err != 0) return -err;
                // errno == 0 with ret < 0 is unexpected; retry conservatively
            }
        }
    }

public:
    shared_mutex() = default;
    shared_mutex(const shared_mutex&) = delete;
    shared_mutex& operator=(const shared_mutex&) = delete;

    bool trylock() {
        int64_t expected = 0;
        if (lock_state.compare_exchange_strong(
                expected, WRITE_LOCKED,
                std::memory_order_acq_rel, std::memory_order_relaxed)) {
            write_lock_owner = photon::CURRENT;
            return true;
        }
        return false;
    }

    bool trylock_shared() {
        auto state = lock_state.load(std::memory_order_acquire);
        while (state >= 0 && state < MAX_SHARED_LOCK_COUNT) {
            if (lock_state.compare_exchange_weak(
                    state, state + 1,
                    std::memory_order_acq_rel, std::memory_order_acquire))
                return true;
        }
        return false;
    }

    int lock(uint64_t timeout = -1) {
        return do_lock([this] { return trylock(); }, cv_unique, timeout);
    }

    int lock_shared(uint64_t timeout = -1) {
        return do_lock([this] { return trylock_shared(); }, cv_shared, timeout);
    }

    void unlock() {
        SCOPED_LOCK(spin);
        assert(lock_state.load(std::memory_order_relaxed) == WRITE_LOCKED);
        assert(write_lock_owner == photon::CURRENT);
        write_lock_owner = nullptr;
        lock_state.store(0, std::memory_order_release);
        try_wake();
    }

    // CRITICAL: Must unconditionally call try_wake when prev == 1.
    // Between fetch_sub and acquiring spinlock, a new reader may sneak in
    // via the lock-free trylock_shared() fast path. Double-checking state
    // here would miss that reader and leave waiting writers stuck forever.
    void unlock_shared() {
        auto prev = lock_state.fetch_sub(1, std::memory_order_acq_rel);
        assert(prev > 0);  // must hold a shared lock; prev<=0 indicates misuse
        if (prev == 1) {
            SCOPED_LOCK(spin);
            try_wake();
        }
    }

    // Debug snapshot; may be stale immediately after return.
    photon::thread* get_write_lock_owner() const {
        if (lock_state.load(std::memory_order_acquire) == WRITE_LOCKED)
            return write_lock_owner;
        return nullptr;
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