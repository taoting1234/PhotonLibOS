#include <photon/alilfs-extend/packaged_task.h>
#include <photon/common/alog.h>
#include <photon/photon.h>
#include <photon/thread/workerpool.h>

int main() {
    photon::init();
    DEFER(photon::fini());

    set_log_output(log_output_stdout);
    set_log_output_level(ALOG_DEBUG);
    DEFER(set_log_output(log_output_null));
    {
        auto t =
            photon_lfsextend::packaged_task<int(int, int&)>([](int a, int& b) {
                LOG_INFO("Fire", VALUE(a), VALUE(b));
                photon::thread_usleep(1000000);
                return 42;
            });

        auto f = t.get_future();
        photon::thread_create11([t = std::move(t)]() mutable {
            int a = 1, b = 2;
            t(a, b);
        });

        LOG_INFO(VALUE(f.get()));
    }

    {
        photon::WorkPool wp(1, 0, 0, -1);
        auto t = photon_lfsextend::packaged_task<int()>([]() {
            LOG_INFO("Fire");
            photon::thread_usleep(1000000);
            return 42;
        });

        auto f = t.get_future();
        wp.async_call(new auto([t = std::move(t)]() mutable { t(); }));

        auto status = f.wait_for(std::chrono::milliseconds(10));
        LOG_INFO("wait 1 ms status = ", (int)status);
        status = f.wait_for(std::chrono::seconds(1));
        LOG_INFO("wait 1 sec status = ", (int)status);
        LOG_INFO(VALUE(f.get()));
    }

    {
        auto t = photon_lfsextend::packaged_task<void()>([]() {
            LOG_INFO("Fire");
            std::this_thread::sleep_for(std::chrono::seconds(1));
        });
        auto f = t.get_future();
        auto th = std::thread([t = std::move(t)]() mutable { t(); });
        th.join();
        f.get();
        LOG_INFO("DONE");
    }

    {
        auto t = photon_lfsextend::packaged_task<int()>([]() {
            LOG_INFO("Throw");
            throw std::runtime_error("test");
            return 42;
        });

        auto f = t.get_future();
        photon::thread_create11([t = std::move(t)]() mutable { t(); });

        try {
            f.get();
        } catch (std::exception& e) {
            LOG_ERROR("Caught exception: ", e.what());
        }
    }

    {  // also able to work in non-photon environment
        std::thread wth([] {
            std::atomic_bool done(false);
            auto t = photon_lfsextend::packaged_task<void()>([&]() {
                LOG_INFO("Fire");
                std::this_thread::sleep_for(std::chrono::seconds(1));
                done = true;
            });
            auto f = t.get_future();
            auto th = std::thread([t = std::move(t)]() mutable { t(); });
            th.join();
            f.get();
            LOG_INFO("DONE");
        });
        wth.join();
    }

    return 0;
}