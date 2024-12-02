#pragma once
#include <photon/common/timeout.h>
#include <photon/thread/std-compat.h>
#include <photon/thread/thread.h>

#include <chrono>
#include <future>

namespace photon_lfsextend {

template <typename R>
class future;

template <typename R>
class promise {
    std::promise<R> _prom;
    std::shared_ptr<photon::semaphore> _sem;

public:
    promise() : _sem(std::make_shared<photon::semaphore>()) {}
    template <class Alloc>
    explicit promise(std::allocator_arg_t, const Alloc& alloc)
        : _prom(std::allocator_arg, alloc),
          _sem(std::make_shared<photon::semaphore>()) {}
    promise(promise&& other) noexcept = default;
    promise(const promise& other) = delete;
    promise& operator=(promise&& other) noexcept = default;
    promise& operator=(const promise& rhs) = delete;

    void set_exception(std::exception_ptr p) {
        _prom.set_exception(p);
        _sem->signal(1);
    }

    template <typename... Anytype>
    void set_value(Anytype... args) {
        _prom.set_value(args...);
        _sem->signal(1);
    }

    future<R> get_future();

    void swap(promise<R>& rhs) noexcept {
        _prom.swap(rhs._prom);
        _sem.swap(rhs._sem);
    }
};

template <typename R>
class future {
    std::future<R> _fut;
    std::shared_ptr<photon::semaphore> _sem;

public:
    future() : _fut(), _sem(std::make_shared<photon::semaphore>()) {}
    explicit future(std::future<R>&& f, std::shared_ptr<photon::semaphore> sem)
        : _fut(std::move(f)), _sem(std::move(sem)) {}
    future(const future&) = delete;
    future(future&&) noexcept = default;
    future& operator=(const future&) = delete;
    future& operator=(future&&) noexcept = default;

    bool valid() const noexcept { return _fut.valid(); }
    void wait() const {
        if (_fut.wait_for(std::chrono::microseconds(0)) ==
            std::future_status::ready)
            return;
        if (photon::CURRENT) _sem->wait(1);
        _fut.wait();
    }

    template <class Rep, class Period>
    std::future_status wait_for(
        const std::chrono::duration<Rep, Period>& timeout_duration) const {
        if (_fut.wait_for(std::chrono::microseconds(0)) ==
            std::future_status::ready)
            return std::future_status::ready;
        if (photon::CURRENT) {
            auto ret = _sem->wait(
                1, photon_std::__duration_to_microseconds(timeout_duration));
            if (ret == 0) return _fut.wait_for(std::chrono::microseconds(0));
            return std::future_status::timeout;
        }
        return _fut.wait_for(timeout_duration);
    }

    template <class Clock, class Duration>
    std::future_status wait_until(
        const std::chrono::time_point<Clock, Duration>& timeout_time) const {
        if (_fut.wait_for(std::chrono::microseconds(0)) ==
            std::future_status::ready)
            return std::future_status::ready;
        if (photon::CURRENT) {
            auto ret = _sem->wait(1, photon_std::__duration_to_microseconds(
                                         timeout_time - Clock::now()));
            if (ret == 0) return _fut.wait_for(std::chrono::microseconds(0));
            return std::future_status::timeout;
        }
        return _fut.wait_until(timeout_time);
    }
    R get() {
        wait();
        return _fut.get();
    }
};

template <typename R>
future<R> promise<R>::get_future() {
    return future<R>(_prom.get_future(), _sem);
}

template <typename Signature>
class packaged_task;

template <typename R, typename... Args>
class packaged_task<R(Args...)> {
    std::packaged_task<R(Args...)> _pt;
    std::shared_ptr<photon::semaphore> _sem;

public:
    packaged_task(const packaged_task&) = delete;
    packaged_task(packaged_task&& rhs) noexcept = default;
    packaged_task& operator=(const packaged_task&) = delete;
    packaged_task& operator=(packaged_task&& rhs) noexcept = default;

    template <typename Func>
    explicit packaged_task(Func&& func)
        : _pt(std::forward<Func>(func)),
          _sem(std::make_shared<photon::semaphore>()) {}

    template <class Func, class Allocator>
    explicit packaged_task(std::allocator_arg_t, const Allocator& a, Func&& f)
        : _pt(std::allocator_arg, a, std::forward<Func>(f)) {}
    bool valid() noexcept { return _pt.valid(); }
    future<R> get_future() { return future<R>(_pt.get_future(), _sem); }
    void operator()(Args... args) {
        _pt(args...);
        _sem->signal(1);
    }

    void reset() {
        _pt.reset();
        _sem = std::make_shared<photon::semaphore>();
    }

    void swap(packaged_task& rhs) noexcept {
        _pt.swap(rhs._pt);
        _sem.swap(rhs._sem);
    }
};

}  // namespace photon_lfsextend

namespace std {
template <typename R, typename... Args>
void swap(photon_lfsextend::packaged_task<R(Args...)>& lhs,
          photon_lfsextend::packaged_task<R(Args...)>& rhs) noexcept {
    lhs.swap(rhs);
}

template <typename R>
void swap(photon_lfsextend::promise<R>& lhs,
          photon_lfsextend::promise<R>& rhs) noexcept {
    lhs.swap(rhs);
}
}  // namespace std