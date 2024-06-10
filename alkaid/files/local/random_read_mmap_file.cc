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
#include <alkaid/files/local/random_read_mmap_file.h>
#include <alkaid/files/local/sys_io.h>

namespace alkaid::lfs {
    RandomReadMMapFile::~RandomReadMMapFile() {
        auto r = close_impl();
        (void)r;
    }

    turbo::Status RandomReadMMapFile::open(const std::string &filename, std::any options, FileEventListener listener) noexcept {
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
        return turbo::unavailable_error("open file failed");
    }

    turbo::Status RandomReadMMapFile::close() noexcept {
        return close_impl();
    }

    turbo::Result<int64_t> RandomReadMMapFile::tell() const noexcept {
        return 0;
    }

    turbo::Result<size_t> RandomReadMMapFile::size() const noexcept {
        if(mmap_source_.is_open()) {
            return mmap_source_.size();
        }
        return turbo::unavailable_error("file not open");
    }

    turbo::Result<size_t> RandomReadMMapFile::read_at_impl(int64_t offset, void *buff, size_t len) noexcept {
           if(!mmap_source_.is_open()) {
                return turbo::unavailable_error("file not open");
            }
            if(offset < 0 || offset >= mmap_source_.size()) {
                return turbo::invalid_argument_error("offset out of range");
            }
            if(len == 0) {
                return 0;
            }
            if(offset + len > mmap_source_.size()) {
                len = mmap_source_.size() - offset;
            }
            std::memcpy(buff, mmap_source_.data() + offset, len);
            return len;
    }

    turbo::Status RandomReadMMapFile::close_impl() noexcept {
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

}  // namespace alkaid::lfs
