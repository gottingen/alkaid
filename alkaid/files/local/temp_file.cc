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
// Created by jeff on 24-6-9.
//
#include <alkaid/files/local/temp_file.h>
#include <alkaid/files/local/defines.h>
#include <alkaid/files/ghc/filesystem.hpp>
#include <alkaid/files/local/sys_io.h>
#include <turbo/strings/substitute.h>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <random>
#include <mutex>

namespace alkaid::lfs {

    static std::mt19937 temp_file_bit_gen(std::random_device{}());
    static std::mutex temp_file_spin_lock;

    std::string TempFile::generate_temp_file_name(std::string_view prefix, std::string_view ext, size_t bits) {
        std::string gen_name;
        std::uniform_int_distribution<char> uniform('a', 'z');
        {
            std::lock_guard lock(temp_file_spin_lock);
            for (size_t i = 0; i < bits; ++i) {
                gen_name.push_back(uniform(temp_file_bit_gen));
            }
        }
        std::string result;
        if (!ext.empty()) {
            result = turbo::substitute("$0$1.$2", prefix, gen_name, ext);
        } else {
            result = turbo::str_cat(prefix, gen_name);
        }
        return result;
    }

    TempFile::~TempFile() {
        auto r = close_impl();
        (void) r;
    }

    turbo::Status TempFile::open(const std::string &path, std::any options, FileEventListener listener) noexcept {
        auto r = close_impl();
        if (!r.ok()) {
            return r;
        }
        path_ = generate_temp_file_name(path, "tmp", 6);
        auto rs = open_file(path, kDefaultTruncateWriteOption);
        if (!rs.ok()) {
            return rs.status();
        }
        _fd = rs.value();
        return turbo::OkStatus();
    }

    turbo::Result<int64_t> TempFile::tell() const noexcept {
        if (_fd == INVALID_FILE_HANDLER) {
            return turbo::unavailable_error("file not opened");
        }
        auto pos = lseek(_fd, 0, SEEK_CUR);
        if (pos == -1) {
            return turbo::errno_to_status(errno, "Failed to get current position of file %s", path_);
        }
        return pos;
    }

    turbo::Result<size_t> TempFile::size() const noexcept {
        if (_fd == INVALID_FILE_HANDLER) {
            return turbo::unavailable_error("file not opened");
        }
        auto r = file_size(_fd);
        if (r < 0) {
            return turbo::errno_to_status(errno, "get file size failed");
        }
        return r;
    }

    turbo::Status TempFile::append_impl(const void *buff, size_t len) noexcept {
        if (_fd == INVALID_FILE_HANDLER) {
            return turbo::unavailable_error("file not opened");
        }
        auto n = sys_write(_fd, buff, len);
        if (n == -1) {
            return turbo::errno_to_status(errno, "Failed to write to file %s", path_);
        }
        return turbo::OkStatus();
    }

    turbo::Status TempFile::close_impl() noexcept {
        if (_fd != INVALID_FILE_HANDLER) {
            auto rs = ::close(_fd);
            if (rs == -1) {
                return turbo::errno_to_status(errno, "Failed to close file %s", path_);
            }
            _fd = INVALID_FILE_HANDLER;
        }
        std::error_code ec;
        alkaid::filesystem::remove(path_, ec);
        return turbo::OkStatus();
    }

    turbo::Status TempFile::truncate(size_t size) noexcept {
        if (_fd == INVALID_FILE_HANDLER) {
            return turbo::unavailable_error("file not opened");
        }
        if (::ftruncate(_fd, static_cast<off_t>(size)) != 0) {
            return turbo::errno_to_status(errno, "Failed truncate file %s for size:%ld ", path_,
                                          static_cast<off_t>(size));
        }
        if (::lseek(_fd, static_cast<off_t>(size), SEEK_SET) != 0) {
            return turbo::errno_to_status(errno, "Failed seek file end %s for size:%ld ", path_,
                                          static_cast<off_t>(size));
        }
        return turbo::OkStatus();
    }

}  // namespace alkaid::lfs