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
#include <alkaid/files/sequential_read_file.h>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>

namespace alkaid {

    SequentialReadFile::SequentialReadFile(const FileEventListener &listener)
            : _listener(listener) {

    }

    SequentialReadFile::~SequentialReadFile() {
        close();
    }

    collie::Status
    SequentialReadFile::open(const collie::filesystem::path &path, const OpenOption &option) noexcept {
        close();
        _option = option;
        _file_path = path;
        if(_file_path.empty()) {
            return collie::Status::invalid_argument("file path is empty");
        }
        if (_listener.before_open) {
            _listener.before_open(_file_path);
        }

        for (int tries = 0; tries < _option.open_tries; ++tries) {
            auto rs = open_file(_file_path, _option);
            if (rs.ok()) {
                _fd = rs.value_or_die();
                if (_listener.after_open) {
                    _listener.after_open(_file_path, _fd);
                }
                return collie::Status::ok_status();
            }
            if (_option.open_interval_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(_option.open_interval_ms));
            }
        }
        return collie::Status::from_errno(errno, "Failed opening file {} for reading", _file_path.c_str());
    }

    collie::Result<size_t> SequentialReadFile::read(void *buff, size_t len) {
        INVALID_FD_RETURN(_fd);
        if (len == 0) {
            return 0;
        }
        auto nread = sys_read(_fd, buff, len);
        if(nread < 0) {
            return collie::Status::from_errno(errno, "read file {} failed", _file_path.c_str());
        }
        _position += nread;
        return nread;
    }

    collie::Result<size_t> SequentialReadFile::read(std::string *content, size_t n) {
        INVALID_FD_RETURN(_fd);
        size_t len = n;
        if (len == kInfiniteFileSize) {
            auto r = file_size(_fd);
            if (r == -1) {
                return collie::Status::from_errno(errno, "get file size failed");
            }
            len = r;
            if(len == 0) {
                return 0;
            }
        }
        auto pre_len = content->size();
        content->resize(pre_len + len);
        char *pdata = content->data() + pre_len;
        auto nread = sys_read(_fd, pdata, len);
        if(nread < 0) {
            content->resize(pre_len);
            return collie::Status::from_errno(errno, "read file {} failed", _file_path.c_str());
        }
        _position += nread;
        content->resize(pre_len + nread);
        return nread;
    }

    void SequentialReadFile::close() {
        if (_fd > 0) {
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

    collie::Status SequentialReadFile::skip(off_t n) {
        INVALID_FD_RETURN(_fd);

        ::lseek(_fd, n, SEEK_CUR);
        return collie::Status::ok_status();
    }

    collie::Result<bool> SequentialReadFile::is_eof() const {
        INVALID_FD_RETURN(_fd);
        auto fp = fdopen(_fd, "rb");
        if (fp == nullptr) {
            return collie::Status::from_errno(errno,"test file eof {}", _file_path.c_str());
        }
        auto ret = ::feof(fp);
        if (ret < 0) {
            return collie::Status::from_errno(errno, "test file eof {}", _file_path.c_str());
        }
        return ret;
    }

} // namespace turbo