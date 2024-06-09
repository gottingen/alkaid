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
#include <alkaid/files/local/sequential_write_file.h>
#include <alkaid/files/local/defines.h>
#include <alkaid/ghc/filesystem.hpp>
#include <alkaid/files/local/sys_io.h>

namespace alkaid::lfs {

    SequentialWriteFile::~SequentialWriteFile() {
        auto r = close_impl();
        (void) r;
    }

    turbo::Status SequentialWriteFile::open(const std::string &path, std::any options, FileEventListener listener) noexcept {
        path_ = path;
        listener_ = std::move(listener);
        if (options.has_value()) {
            open_option_ = std::any_cast<OpenOption>(options);
        }
        if (path_.empty()) {
            return turbo::invalid_argument_error("file path is empty");
        }
        if (listener_.before_open) {
            listener_.before_open(this);
        }
        for (int tries = 0; tries < open_option_.open_tries; ++tries) {
            // create containing folder if not exists already.
            if (open_option_.create_dir_if_miss) {
                auto fpath = ghc::filesystem::path(path_);
                auto pdir = fpath.parent_path();
                if (!pdir.empty()) {
                    std::error_code ec;
                    if (!ghc::filesystem::exists(pdir, ec)) {
                        if (ec) {
                            continue;
                        }
                        if (!ghc::filesystem::create_directories(pdir, ec)) {
                            continue;
                        }
                    }
                }
            }
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
        return turbo::errno_to_status(errno, "Failed opening file {} for writing");
    }

    turbo::Result<int64_t> SequentialWriteFile::tell() const noexcept {
        if (_fd == INVALID_FILE_HANDLER) {
            return turbo::unavailable_error("file not opened");
        }
        auto pos = lseek(_fd, 0, SEEK_CUR);
        if (pos == -1) {
            return turbo::errno_to_status(errno, "Failed to get current position of file %s", path_);
        }
        return pos;
    }

    turbo::Result<size_t> SequentialWriteFile::size() const noexcept {
        if (_fd == INVALID_FILE_HANDLER) {
            return turbo::unavailable_error("file not opened");
        }
        auto r = file_size(_fd);
        if (r < 0) {
            return turbo::errno_to_status(errno, "get file size failed");
        }
        return r;
    }

    turbo::Status SequentialWriteFile::append_impl(const void *buff, size_t len) noexcept {
        if (_fd == INVALID_FILE_HANDLER) {
            return turbo::unavailable_error("file not opened");
        }
        auto n = sys_write(_fd, buff, len);
        if (n == -1) {
            return turbo::errno_to_status(errno, "Failed to write to file %s", path_);
        }
        return turbo::OkStatus();
    }

    turbo::Status SequentialWriteFile::close_impl() noexcept {
        if (_fd != INVALID_FILE_HANDLER) {
            if (listener_.before_close) {
                listener_.before_close(this);
            }
            auto rs = ::close(_fd);
            if (rs == -1) {
                return turbo::errno_to_status(errno, "Failed to close file %s", path_);
            }
            _fd = INVALID_FILE_HANDLER;
            if (listener_.after_close) {
                listener_.after_close(this);
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status SequentialWriteFile::truncate(size_t size) noexcept {
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