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

#include <alkaid/files/interface.h>
#include <alkaid/files/local/defines.h>
#include <alkaid/files/local/random_read_file.h>
#include <alkaid/files/local/sys_io.h>

namespace alkaid::lfs {
    RandomReadFile::~RandomReadFile() {
        auto r = close_impl();
        (void)r;
    }

    turbo::Status RandomReadFile::open(const std::string &filename, std::any options, FileEventListener listener) noexcept {
        auto r = close_impl();
        (void)r;
        if(options.has_value()) {
            try {
                open_option_ = std::any_cast<OpenOption>(options);
            } catch (const std::bad_any_cast &e) {
                return turbo::invalid_argument_error("invalid options");
            }
        }
        listener_ = listener;
        path_ = filename;
        if(path_.empty()) {
            return turbo::invalid_argument_error("file path is empty");
        }
        if (listener_.before_open) {
            listener_.before_open(this);
        }

        for (int tries = 0; tries < open_option_.open_tries; ++tries) {
            auto rs = open_file(path_, open_option_);
            if (rs.ok()) {
                _fd = rs.value();
                if (listener_.after_open) {
                    listener_.after_open(this);
                }
                return turbo::OkStatus();
            }
            if (open_option_.open_interval_ms > 0) {
                turbo::sleep_for(turbo::Duration::milliseconds(open_option_.open_interval_ms));
            }
        }
        return turbo::unavailable_error("open file failed");
    }

    turbo::Status RandomReadFile::close() noexcept {
        return close_impl();
    }

    turbo::Result<int64_t> RandomReadFile::tell() const noexcept {
        if (_fd == INVALID_FILE_HANDLER) {
            return turbo::invalid_argument_error("file not open");
        }
        return ::lseek(_fd, 0, SEEK_CUR);
    }

    turbo::Result<size_t> RandomReadFile::size() const noexcept {
        if (_fd == INVALID_FILE_HANDLER) {
            return turbo::unavailable_error("file not opened");
        }
        auto r = file_size(_fd);
        if (r < 0) {
            return turbo::errno_to_status(errno, "get file size failed");
        }
        return r;
    }

    turbo::Result<size_t> RandomReadFile::read_at_impl(int64_t offset, void *buff, size_t len) noexcept {
        INVALID_FD_RETURN(_fd);
        size_t has_read = 0;
        /// _fd may > 0 with _fp valid
        ssize_t read_size = sys_pread(_fd, buff, len, static_cast<off_t>(offset));
        if(read_size < 0 ) {
            return turbo::errno_to_status(errno, "Failed reading file  for reading");
        }
        // read_size > 0 means read the end of file
        return has_read;
    }

    turbo::Status RandomReadFile::close_impl() noexcept {
        if (_fd > 0) {
            if (listener_.before_close) {
                listener_.before_close(this);
            }

            ::close(_fd);
            _fd = INVALID_FILE_HANDLER;

            if (listener_.after_close) {
                listener_.after_close(this);
            }
        }
        return turbo::OkStatus();
    }

}  // namespace alkaid::lfs
