#pragma once

#include <fcntl.h>
#include <limits.h>
#include <photon/common/alog.h>
#include <sys/uio.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <tuple>

/**
Log output to file with rotate but not number filename.

Kept only log_file_max_cnt log files. current log file have no special suffix,
Old log fill goes trying with gzip.
**/

class LogOutputCompressFile final : public ILogOutput {
public:
    int log_file_fd = -1;
    uint64_t log_file_size_limit = 0;
    char* log_file_name = nullptr;
    std::atomic<uint64_t> log_file_size{0};
    unsigned int log_file_max_cnt = 10;

    // Dtor has been set as private member of interface
    // calling `destruct` is only way to destruct logoutput object
    virtual void destruct() override {
        log_output_file_close();
        delete this;
    }

    // throttle shows no meaning
    virtual uint64_t set_throttle(uint64_t t = -1UL) override { return -1; }
    virtual uint64_t get_throttle() override { return -1; }

    virtual int get_log_file_fd() override { return log_file_fd; }

    int fopen(const char* fn) {
        auto mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        return open(fn, O_CREAT | O_WRONLY | O_APPEND, mode);
    }

    void write(int, const char* begin, const char* end) override {
        if (log_file_fd < 0) return;
        uint64_t length = end - begin;
        iovec iov{(void*)begin, length};
#ifndef _WIN64
        std::ignore =
            ::writev(log_file_fd, &iov,
                     1);  // writev() is atomic, whereas write() is not
#else
        std::ignore = ::write(log_file_fd, iov.iov_base, iov.iov_len);
#endif
        if (log_file_name && log_file_size_limit) {
            log_file_size += length;
            if (log_file_size > log_file_size_limit) {
                static std::mutex log_file_lock;
                std::lock_guard<std::mutex> guard(log_file_lock);
                if (log_file_size > log_file_size_limit) {
                    log_file_rotate();
                    reopen_log_output_file();
                }
            }
        }
    }

    void log_output_file_setting(int fd) {
        if (fd < 0) return;
        if (log_file_fd > 2 && log_file_fd != fd) ::close(log_file_fd);

        log_file_fd = fd;
        log_file_size.store(lseek(fd, 0, SEEK_END));
        free(log_file_name);
        log_file_name = nullptr;
        log_file_size_limit = 0;
    }

    int log_output_file_setting(const char* fn, uint64_t rotate_limit,
                                int max_log_files) {
        int fd = fopen(fn);
        if (fd < 0) return -1;

        log_output_file_setting(fd);
        free(log_file_name);
        log_file_name = strdup(fn);
        log_file_size_limit = std::max(rotate_limit, (uint64_t)(1024 * 1024));
        log_file_max_cnt = std::min(max_log_files, 30);

        // pickup all old log files

        return 0;
    }

    void reopen_log_output_file() {
        int fd = fopen(log_file_name);
        if (fd < 0) {
            static char msg[] = "failed to open log output file: ";
            std::ignore = ::write(log_file_fd, msg, sizeof(msg) - 1);
            if (log_file_name)
                std::ignore =
                    ::write(log_file_fd, log_file_name, strlen(log_file_name));
            std::ignore = ::write(log_file_fd, "\n", 1);
            return;
        }

        log_file_size = 0;
        dup2(fd, log_file_fd);  // to make sure log_file_fd
        close(fd);              // doesn't change
    }

    static inline void add_generation(char* buf, int size,
                                      unsigned int generation,
                                      bool gz = false) {
        if (generation == 0) {
            buf[0] = '\0';
        } else {
            snprintf(buf, size, ".%u%s", generation, gz ? ".gz" : "");
        }
    }

    void log_file_rotate() {
        if (!log_file_name || access(log_file_name, F_OK) != 0) return;

        int fn_length = (int)strlen(log_file_name);
        char fn0[PATH_MAX], fn1[PATH_MAX];
        strcpy(fn0, log_file_name);
        strcpy(fn1, log_file_name);

        unsigned int last_generation = 1;  // not include
        while (true) {
            add_generation(fn0 + fn_length, sizeof(fn0) - fn_length,
                           last_generation, true);
            if (0 != access(fn0, F_OK)) break;
            last_generation++;
        }

        while (last_generation >= 1) {
            add_generation(fn0 + fn_length, sizeof(fn0) - fn_length,
                           last_generation - 1, true);
            add_generation(fn1 + fn_length, sizeof(fn1) - fn_length,
                           last_generation, last_generation > 1);

            if (last_generation >= log_file_max_cnt) {
                add_generation(fn0 + fn_length, sizeof(fn0) - fn_length,
                               last_generation - 1, last_generation > 1);
                unlink(fn0);
            } else {
                rename(fn0, fn1);
                if (last_generation == 1) {
                    std::string fn(fn1);
                    system(("gzip -f " + fn).c_str());
                }
            }
            last_generation--;
        }
    }

    int log_output_file_close() {
        if (log_file_fd < 0) {
            errno = EALREADY;
            return -1;
        }
        close(log_file_fd);
        log_file_fd = -1;
        free(log_file_name);
        log_file_name = nullptr;
        return 0;
    }
};

inline ILogOutput* new_file_compress_log_output(
    const char* fn, uint64_t rotate_limit = UINT64_MAX,
    int max_log_files = 10) {
    auto ret = new LogOutputCompressFile();
    ret->log_output_file_setting(fn, rotate_limit, max_log_files);
    return ret;
}