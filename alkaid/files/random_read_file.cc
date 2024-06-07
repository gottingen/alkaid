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
#include <alkaid/files/random_read_file.h>
#include <turbo/times/time.h>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>

namespace alkaid {

    RandomReadFile::RandomReadFile(const FileEventListener &listener)
            : _listener(listener) {

    }

    RandomReadFile::~RandomReadFile() {
        this->close();
    }

    turbo::Status
    RandomReadFile::open(const ghc::filesystem::path &path, const OpenOption &option) noexcept {
        this->close();
        _option = option;
        _file_path = path;
        if(_file_path.empty()) {
            return turbo::invalid_argument_error("file path is empty");
        }
        if (_listener.before_open) {
            _listener.before_open(_file_path);
        }

        for (int tries = 0; tries < _option.open_tries; ++tries) {
            auto rs = open_file(_file_path, _option);
            if (rs.ok()) {
                _fd = rs.value();
                if (_listener.after_open) {
                    _listener.after_open(_file_path, _fd);
                }
                return turbo::OkStatus();
            }
            if (_option.open_interval_ms > 0) {
                turbo::sleep_for(turbo::Duration::milliseconds(_option.open_interval_ms));
            }
        }
        return turbo::errno_to_status(errno, "Failed opening file for reading");
    }

    turbo::Result<size_t> RandomReadFile::read(off_t offset, void *buff, size_t len) {
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

    turbo::Result<size_t> RandomReadFile::read(off_t offset, std::string *content, size_t n) {
        INVALID_FD_RETURN(_fd);
        size_t len = n;
        if(len == kInfiniteFileSize) {
            auto r = file_size(_fd);
            if(r == -1) {
                return turbo::errno_to_status(errno, "get file size failed");
            }
            len = r - offset;
            if(len <= 0) {
                return turbo::invalid_argument_error("bad offset");
            }
        }
        auto pre_len = content->size();
        content->resize(pre_len + len);
        char* pdata = content->data() + pre_len;
        auto rs = sys_pread(_fd, pdata, len, offset);
        if(rs < 0) {
            content->resize(pre_len);
            return turbo::errno_to_status(errno, "Failed reading file for reading");
        }
        content->resize(pre_len + rs);
        return rs;
    }

    void RandomReadFile::close() {
        if (_fd != INVALID_FILE_HANDLER) {
            if (_listener.before_close) {
                _listener.before_close(_file_path, _fd);
            }

            ::close(_fd);
            _fd = INVALID_FILE_HANDLER;

            if (_listener.after_close) {
                _listener.after_close(_file_path);
            }
        }
    }


} // namespace alkaid
