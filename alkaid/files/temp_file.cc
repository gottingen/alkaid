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
// Created by jeff on 24-5-21.
//


#include <alkaid/files/internal/sys_io.h>
#include <alkaid/files/temp_file.h>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <random>
#include <mutex>

// Initializing array. Needs to be macro.
#define BASE_FILES_TEMP_FILE_PATTERN "temp_file_XXXXXX"

namespace alkaid {

    static std::mt19937 temp_file_bit_gen(std::random_device{}());
    static std::mutex temp_file_spin_lock;

    std::string TempFile::generate_temp_file_name(std::string_view prefix, std::string_view ext, size_t bits) {
        std::string gen_name;
        std::uniform_int_distribution<char> uniform('a', 'z');
        {
            std::lock_guard lock(temp_file_spin_lock);
            for(size_t i = 0; i < bits; ++i) {
                gen_name.push_back(uniform(temp_file_bit_gen));
            }
        }
        std::string result;
        if(!ext.empty()) {
            result = collie::format("{}{}.{}", prefix, gen_name, ext);
        } else {
            result = collie::format("{}{}", prefix, gen_name);
        }
        return result;
    }

    TempFile::TempFile(const FileEventListener &listener) :_file(listener) {

    }

    [[nodiscard]] collie::Status TempFile::open(std::string_view prefix, std::string_view ext, size_t bits) noexcept {
        if(_ever_opened) {
            return collie::Status::ok_status();
        }
        _file_path = generate_temp_file_name(prefix, ext, bits);
        auto rs = _file.open(_file_path,kDefaultTruncateWriteOption);
        if(!rs.ok()) {
            return rs;
        }
        _ever_opened = true;
        return collie::Status::ok_status();
    }

    // Write until all buffer was written or an error except EINTR.
    // Returns:
    //    -1   error happened, errno is set
    // count   all written
    static ssize_t temp_file_write_all(int fd, const void *buf, size_t count) {
        size_t off = 0;
        for (;;) {
            ssize_t nw = write(fd, (char *) buf + off, count - off);
            if (nw == (ssize_t) (count - off)) {  // including count==0
                return count;
            }
            if (nw >= 0) {
                off += nw;
            } else if (errno != EINTR) {
                return -1;
            }
        }
    }

    collie::Status TempFile::write(const void *buf, size_t count) {
        if(!_ever_opened) {
            return collie::Status::unavailable("TempFile not opened");
        }
        return _file.write(buf, count);
    }


} // namespace alkaid
