#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <photon/alilfs-extend/packaged_task.h>
#include <photon/alilfs-extend/shared_mutex.h>
#include <photon/common/alog.h>
#include <photon/photon.h>
#include <photon/thread/thread.h>
#include <photon/thread/workerpool.h>

thread_local uint64_t rw_count;
thread_local bool writing = false;
thread_local photon_std::shared_mutex rwl;

void* shared_mutex_test(void* args) {
    uint64_t carg = (uint64_t)args;
    auto mode = carg & ((1UL << 32) - 1);
    auto id = carg >> 32;
    // LOG_DEBUG("locking ", VALUE(id), VALUE(mode));
    if (mode == photon::WLOCK) {
        rwl.lock();
        putchar('W');
    } else {
        rwl.lock_shared();
        putchar('R');
    }
    LOG_DEBUG("locked ", VALUE(id), VALUE(mode));
    rw_count++;
    if (mode == photon::RLOCK)
        EXPECT_FALSE(writing);
    else
        writing = true;
    photon::thread_usleep(100 * 1000);
    if (mode == photon::WLOCK) writing = false;
    LOG_DEBUG("unlocking ", VALUE(id), VALUE(mode));
    if (mode == photon::WLOCK) {
        putchar('w');
        rwl.unlock();
    } else {
        putchar('r');
        rwl.unlock_shared();
    }
    // LOG_DEBUG("unlocked ", VALUE(id), VALUE(mode));
    return NULL;
}

TEST(shared_mutex, checklock) {
    std::vector<photon::join_handle*> handles;
    rw_count = 0;
    writing = false;
    for (uint64_t i = 0; i < 100; i++) {
        uint64_t arg =
            (i << 32) | (rand() % 10 < 7 ? photon::RLOCK : photon::WLOCK);
        handles.emplace_back(photon::thread_enable_join(
            photon::thread_create(&shared_mutex_test, (void*)arg, 64 * 1024)));
    }
    for (auto& x : handles) photon::thread_join(x);
    puts("");
    EXPECT_EQ(100UL, rw_count);
}

TEST(packaged_task, basic) {
    auto t = photon_lfsextend::packaged_task<int(int, int&)>([](int a, int& b) {
        LOG_INFO("Fire", VALUE(a), VALUE(b));
        photon::thread_usleep(1000000);
        return 42;
    });

    auto f = t.get_future();
    photon::thread_create11([t = std::move(t)]() mutable {
        int a = 1, b = 2;
        t(a, b);
    });
    auto ret = f.get();
    LOG_INFO(VALUE(ret));
    EXPECT_EQ(42, ret);
}

TEST(packaged_task, workpool) {
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
    auto ret = f.get();
    LOG_INFO(VALUE(ret));
    EXPECT_EQ(42, ret);
}

TEST(packaged_task, thread) {
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
    EXPECT_TRUE(done.load());
    LOG_INFO("DONE");
}

TEST(packaged_task, exception) {
    auto t = photon_lfsextend::packaged_task<int()>([]() {
        LOG_INFO("Throw");
        throw std::runtime_error("test");
        return 42;
    });

    auto f = t.get_future();
    photon::thread_create11([t = std::move(t)]() mutable { t(); });

    int ret;
    bool exc = false;
    try {
        ret = f.get();
    } catch (std::exception& e) {
        EXPECT_STREQ("test", e.what());
        LOG_INFO("Caught exception: ", e.what());
        exc = true;
    }
    (void)ret;
    EXPECT_TRUE(exc);
}

TEST(packaged_task, as_normal_thread) {
    std::thread wth([] {
        EXPECT_EQ(nullptr, photon::CURRENT);
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
        EXPECT_TRUE(done.load());
        LOG_INFO("DONE");
    });
    wth.join();
}

int main(int argc, char** arg) {
    ::testing::InitGoogleTest(&argc, arg);
    photon::init();
    DEFER(photon::fini());
    default_logger.log_output = log_output_stdout;
    set_log_output_level(ALOG_INFO);
    DEFER(default_logger.log_output = log_output_null);

    return RUN_ALL_TESTS();
}