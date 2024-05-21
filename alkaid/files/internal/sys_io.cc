//
// Copyright (C) 2024 EA group inc.
// Author: Jeff.li lijippy@163.com
// All rights reserved.
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
//
//

#include <alkaid/files/internal/sys_io.h>
#include <alkaid/files/fd_guard.h>
#include <collie/port/port.h>
#include <collie/log/logging.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace alkaid::system_internal {
    typedef ssize_t (*iov_function)(int fd, const struct iovec *vector,
                                    int count, off_t offset);

    static ssize_t user_preadv(int fd, const struct iovec *vector,
                               int count, off_t offset) {
        ssize_t total_read = 0;
        for (int i = 0; i < count; ++i) {
            const ssize_t rc = ::pread(fd, vector[i].iov_base, vector[i].iov_len, offset);
            if (rc <= 0) {
                return total_read > 0 ? total_read : rc;
            }
            total_read += rc;
            offset += rc;
            if (rc < (ssize_t) vector[i].iov_len) {
                break;
            }
        }
        return total_read;
    }

    static ssize_t user_pwritev(int fd, const struct iovec *vector,
                                int count, off_t offset) {
        ssize_t total_write = 0;
        for (int i = 0; i < count; ++i) {
            const ssize_t rc = ::pwrite(fd, vector[i].iov_base, vector[i].iov_len, offset);
            if (rc <= 0) {
                return total_write > 0 ? total_write : rc;
            }
            total_write += rc;
            offset += rc;
            if (rc < (ssize_t) vector[i].iov_len) {
                break;
            }
        }
        return total_write;
    }


#if defined(COLLIE_PROCESSOR_X86_64)

#ifndef SYS_preadv
#define SYS_preadv 295
#endif  // SYS_preadv

#ifndef SYS_pwritev
#define SYS_pwritev 296
#endif // SYS_pwritev

    // SYS_preadv/SYS_pwritev is available since Linux 2.6.30
    static ssize_t sys_preadv(int fd, const struct iovec *vector,
                              int count, off_t offset) {
        return syscall(SYS_preadv, fd, vector, count, offset);
    }

    static ssize_t sys_pwritev(int fd, const struct iovec *vector,
                               int count, off_t offset) {
        return syscall(SYS_pwritev, fd, vector, count, offset);
    }

    inline iov_function get_preadv_func() {
#if defined(TURBO_PLATFORM_OSX)
        return user_preadv;
#endif
        FDGuard fd(::open("/dev/zero", O_RDONLY));
        if (fd < 0) {
            LOG(WARN) << "Fail to open /dev/zero";
            return user_preadv;
        }
        char dummy[1];
        iovec vec = {dummy, sizeof(dummy)};
        const int rc = syscall(SYS_preadv, (int) fd, &vec, 1, 0);
        if (rc < 0) {
            LOG(WARN)<< "The kernel doesn't support SYS_preadv, use user_preadv instead";
            return user_preadv;
        }
        return sys_preadv;
    }

    inline iov_function get_pwritev_func() {
        FDGuard fd(::open("/dev/null", O_WRONLY));
        if (fd < 0) {
            LOG(WARN) << "Fail to open /dev/zero";
            return user_pwritev;
        }
#if defined(COLLIE_PLATFORM_OSX)
        return user_pwritev;
#endif
        char dummy[1];
        iovec vec = {dummy, sizeof(dummy)};
        const int rc = syscall(SYS_pwritev, (int) fd, &vec, 1, 0);
        if (rc < 0) {
            LOG(WARN)<< "The kernel doesn't support SYS_preadv, use user_preadv instead";
            return user_pwritev;
        }
        return sys_pwritev;
    }

#else   // COLLIE_PROCESSOR_X86_64

#warning "We don't check if the kernel supports SYS_preadv or SYS_pwritev on non-X86_64, use implementation on pread/pwrite directly."

    inline iov_function get_preadv_func() {
        return user_preadv;
    }

    inline iov_function get_pwritev_func() {
        return user_pwritev;
    }

#endif  // COLLIE_PROCESSOR_X86_64

}  // namespace alkaid::system_internal

namespace alkaid {

    ssize_t sys_pwritev(FILE_HANDLER fd, const struct iovec *vector, int count, off_t offset) {
        static system_internal::iov_function pwritev_func = system_internal::get_pwritev_func();
        return pwritev_func(fd, vector, count, offset);
    }

    ssize_t sys_preadv(FILE_HANDLER fd, const struct iovec *vector, int count, off_t offset) {
        static system_internal::iov_function preadv_func = system_internal::get_preadv_func();
        return preadv_func(fd, vector, count, offset);
    }

    ssize_t sys_pwrite(FILE_HANDLER fd, const void *data, int count, off_t offset) {
        struct iovec iov = {const_cast<void *>(data), static_cast<size_t>(count)};
        return sys_pwritev(fd, &iov, 1, offset);
    }

    ssize_t sys_pread(FILE_HANDLER fd, const void *data, int count, off_t offset) {
        struct iovec iov = {const_cast<void *>(data), static_cast<size_t>(count)};
        return sys_preadv(fd, &iov, 1, offset);
    }

    ssize_t sys_writev(FILE_HANDLER fd, const struct iovec *vector, int count) {
        return ::writev(fd, vector, count);
    }

    ssize_t sys_readv(FILE_HANDLER fd, const struct iovec *vector, int count) {
        return ::readv(fd, vector, count);
    }


    ssize_t sys_write(FILE_HANDLER fd, const void *data, int count) {
        struct iovec iov = {const_cast<void *>(data), static_cast<size_t>(count)};
        return sys_writev(fd, &iov, 1);
    }

    ssize_t sys_read(FILE_HANDLER fd, const void *data, int count) {
        struct iovec iov = {const_cast<void *>(data), static_cast<size_t>(count)};
        return sys_readv(fd, &iov, 1);
    }

    collie::Result<FILE_HANDLER> open_file(const std::string &filename, const OpenOption &option) {
        const FILE_HANDLER fd = ::open((filename.c_str()), option.flags, option.mode);
        if (fd == -1) {
            return collie::Status::from_errno(errno, "Failed opening file {} for reading", filename.c_str());
        }
        return fd;
    }

    collie::Result<FILE_HANDLER> open_file_read(const std::string &filename) {
        return open_file(filename, kDefaultReadOption);
    }

    collie::Result<FILE_HANDLER> open_file_write(const std::string &filename, bool truncate) {
        if (truncate) {
            return open_file(filename, kDefaultTruncateWriteOption);
        }
        return open_file(filename, kDefaultAppendWriteOption);
    }

}

#if defined(COLLIE_PLATFORM_LINUX)
namespace alkaid {
    ssize_t file_size(int fd) {
        if (fd == -1) {
            return -1;
        }
// 64 bits(but not in osx or cygwin, where fstat64 is deprecated)
#    if (defined(__linux__) || defined(__sun) || defined(_AIX)) && (defined(__LP64__) || defined(_LP64))
        struct stat64 st;
        if (::fstat64(fd, &st) == 0) {
            return static_cast<size_t>(st.st_size);
        }
#    else // other unix or linux 32 bits or cygwin
        struct stat st;
        if (::fstat(fd, &st) == 0) {
            return static_cast<size_t>(st.st_size);
        }
#    endif
        return -1;
    }
}  // namespace alkaid
#endif  // COLLIE_PLATFORM_LINUX

#if defined(COLLIE_PLATFORM_WINDOWS)

namespace alkaid {
        ssize_t file_size(int fd) {
        if (fd == -1) {
            return -1;
        }
#    if defined(_WIN64) // 64 bits
                __int64 ret = ::_filelengthi64(fd);
                if (ret >= 0) {
                    return static_cast<size_t>(ret);
                }

#    else // windows 32 bits
                long ret = ::_filelength(fd);
                if (ret >= 0) {
                    return static_cast<size_t>(ret);
                }
#    endif
        return -1;
    }

}  // namespace alkaid
#endif  //  defined(_WIN32) || defined(_WIN64)
