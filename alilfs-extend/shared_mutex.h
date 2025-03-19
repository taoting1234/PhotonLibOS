#pragma once
#include <photon/common/timeout.h>
#include <photon/thread/std-compat.h>
#include <photon/thread/thread.h>

#include <chrono>

namespace photon_lfsextend {

class shared_mutex {
protected:
    constexpr static int64_t MAX_SHARED_LOCK_COUNT = 1 << 16;
    int64_t lock_state;
    photon::condition_variable cv_shared;
    photon::condition_variable cv_unique;
    photon::spinlock spin;

    void try_wake() {
        if (lock_state == 0) {
            if (!cv_unique.notify_one()) cv_shared.notify_all();
        }
    }

    bool prelocked_trylock() {
        if (lock_state == 0) {
            lock_state--;
            return true;
        }
        return false;
    }

    bool prelocked_trylock_shared() {
        if (lock_state >= 0 && lock_state < MAX_SHARED_LOCK_COUNT) {
            lock_state++;
            return true;
        }
        return false;
    }

public:
    shared_mutex() : lock_state(0) {}
    shared_mutex(const shared_mutex&) = delete;
    shared_mutex operator=(const shared_mutex&) = delete;

    bool trylock() {
        SCOPED_LOCK(spin);
        return prelocked_trylock();
    }

    bool trylock_shared() {
        SCOPED_LOCK(spin);
        return prelocked_trylock_shared();
    }

    int lock(uint64_t timeout = -1) {
        photon::Timeout tmo(timeout);
        SCOPED_LOCK(spin);
        while (!prelocked_trylock()) {
            if (cv_unique.wait(spin, tmo.timeout()) == -ETIMEDOUT)
                return -ETIMEDOUT;
        }
        return 0;
    }

    int lock_shared(uint64_t timeout = -1) {
        photon::Timeout tmo(timeout);
        SCOPED_LOCK(spin);
        while (!prelocked_trylock_shared()) {
            if (cv_shared.wait(spin, tmo.timeout()) == -ETIMEDOUT)
                return -ETIMEDOUT;
        }
        return 0;
    }

    int unlock() {
        SCOPED_LOCK(spin);
        if (lock_state >= 0) {
            return -EINVAL;
        }
        lock_state++;
        try_wake();
        return 0;
    }

    int unlock_shared() {
        SCOPED_LOCK(spin);
        if (lock_state <= 0) {
            return -EINVAL;
        }
        lock_state--;
        try_wake();
        return 0;
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
    bool try_lock() { return smtx.trylock() == 0; }
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
    bool try_lock_shared() { return smtx.trylock_shared() == 0; }
    template <class Rep, class Period>
    bool try_lock_shared_for(
        const std::chrono::duration<Rep, Period>& timeout_duration) {
        return smtx.lock_shared(__duration_to_microseconds(timeout_duration)) ==
               0;
    }
    template <class Clock, class Duration>
    bool try_lock_shared_until(
        const std::chrono::time_point<Clock, Duration>& timeout_time);
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
    bool try_lock() { return smtx.trylock() == 0; }
    void unlock() { smtx.unlock(); }
    void lock_shared() {
        auto ret = smtx.lock_shared();
        if (ret < 0) __throw_system_error(ret, "lock failed");
    }
    bool try_lock_shared() { return smtx.trylock_shared() == 0; }
    void unlock_shared() { smtx.unlock_shared(); }
};

}  // namespace photon_std