#include <gtest/gtest.h>
#include <photon/common/alog-stdstring.h>
#include <photon/common/utility.h>
#include <photon/photon.h>
#include <photon/thread/thread11.h>
#include <atomic>
int* get_stack_var_addr() {
    int stack_var = 0;
    int* ret = &stack_var;
    return ret;
}

void access_stack_after_return() {
    EXPECT_DEATH(printf("%d\n", *get_stack_var_addr()), ".*");
}

TEST(photon, stack_use_after_return) {
    auto th = photon::thread_enable_join(photon::thread_create11(access_stack_after_return));
    photon::thread_join(th);
}

void access_dead_thread() {
    std::atomic<int*> var;
    auto th = photon::thread_create11([&]{
        int x = 666;
        var.store(&x);
        printf("%p\n", var.load());
    });
    photon::thread_yield_to(th);
    printf("%p\n", var.load());
    EXPECT_DEATH(printf("%d\n", *var.load()), ".*");
}

TEST(photon, thread_access_after_free) {
    access_dead_thread();
}

extern "C" const char *__asan_default_options() {
    return "detect_stack_use_after_return=1:verbosity=1";
}
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    photon::init();
    DEFER(photon::fini());
    return RUN_ALL_TESTS();
    return 0;
}