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

#include <alkaid/files/local/sequential_read_mmap_file.h>
#include <alkaid/files/local/sys_io.h>

namespace alkaid::lfs {

    SequentialReadMMapFile::~SequentialReadMMapFile() {
        auto r = close_impl();
        (void )r;
    }
    turbo::Status SequentialReadMMapFile::open(const std::string &path, std::any options , FileEventListener listener) noexcept {
        auto r = close_impl();
        pos_ = 0;
        (void )r;
        if(options.has_value()) {
            try {
                open_option_ = std::any_cast<OpenOption>(options);
            } catch (const std::bad_any_cast &e) {
                return turbo::invalid_argument_error("invalid options");
            }
        }
        listener_ = listener;
        path_ = path;
        if(path_.empty()) {
            return turbo::invalid_argument_error("file path is empty");
        }
        if (listener_.before_open) {
            listener_.before_open(this);
        }
        std::error_code ec;
        for (int tries = 0; tries < open_option_.open_tries; ++tries) {
            mmap_source_.map(path_, ec);
            if (!ec) {
                if (listener_.after_open) {
                    listener_.after_open(this);
                }
                return turbo::OkStatus();
            }
            if (open_option_.open_interval_ms > 0) {
                turbo::sleep_for(turbo::Duration::milliseconds(open_option_.open_interval_ms));
            }
        }
        return turbo::errno_to_status(ec.value(), "open file failed");
    }

    turbo::Status SequentialReadMMapFile::close_impl() noexcept {
        if (mmap_source_.is_open()) {
            if (listener_.before_close) {
                listener_.before_close(this);
            }

            mmap_source_.unmap();
            if (listener_.after_close) {
                listener_.after_close(this);
            }
        }
        return turbo::OkStatus();
    }

    turbo::Result<int64_t> SequentialReadMMapFile::tell() const noexcept {
        return pos_;
    }

    turbo::Result<size_t> SequentialReadMMapFile::size() const noexcept {
        if (!mmap_source_.is_open()) {
            return turbo::invalid_argument_error("file not open");
        }
        return mmap_source_.size();
    }

    turbo::Status SequentialReadMMapFile::advance(off_t n) noexcept {
        if (!mmap_source_.is_open()) {
            return turbo::invalid_argument_error("file not open");
        }
        if (n < 0) {
            return turbo::invalid_argument_error("n < 0");
        }
        if (n == 0) {
            return turbo::OkStatus();
        }
        auto new_pos = pos_ + n;
        if (new_pos > mmap_source_.size()) {
            pos_ = mmap_source_.size();
            return turbo::OkStatus();
        }
        pos_ = new_pos;
        return turbo::OkStatus();
    }

    turbo::Result<size_t> SequentialReadMMapFile::read_impl(void *buff, size_t len) noexcept {
        if (!mmap_source_.is_open()) {
            return turbo::invalid_argument_error("file not open");
        }
        if (len == 0) {
            return 0;
        }
        if (pos_ >= mmap_source_.size()) {
            return 0;
        }
        auto nread = std::min(len, mmap_source_.size() - pos_);
        std::memcpy(buff, mmap_source_.data() + pos_, nread);
        pos_ += nread;
        return nread;
    }

}  // namespace alkaid::lfs
