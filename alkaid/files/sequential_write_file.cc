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
#include <alkaid/files/sequential_write_file.h>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>

namespace alkaid {

    SequentialWriteFile::SequentialWriteFile(const FileEventListener &listener) : _listener(listener) {

    }

    SequentialWriteFile::~SequentialWriteFile() {
        close();
    }

    collie::Status SequentialWriteFile::open(const collie::filesystem::path &fname, const OpenOption &option)  noexcept {
        close();
        _option = option;
        _file_path = fname;
        if(_file_path.empty()) {
            return collie::Status::invalid_argument("file path is empty");
        }

        if (_listener.before_open) {
            _listener.before_open(_file_path);
        }
        for (int tries = 0; tries < _option.open_tries; ++tries) {
            // create containing folder if not exists already.
            if (_option.create_dir_if_miss) {
                auto pdir = _file_path.parent_path();
                if (!pdir.empty()) {
                    std::error_code ec;
                    if (!collie::filesystem::exists(pdir, ec)) {
                        if (ec) {
                            continue;
                        }
                        if (!collie::filesystem::create_directories(pdir, ec)) {
                            continue;
                        }
                    }
                }
            }
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
        return collie::Status::from_errno(errno, "Failed opening file {} for writing", _file_path.c_str());
    }

    collie::Status SequentialWriteFile::reopen(bool truncate) {
        close();
        if (_file_path.empty()) {
            return collie::Status::invalid_argument("file path is empty");
        }
        OpenOption option = _option;
        if(truncate) {
            option.truncate();
        }
        return open(_file_path, option);
    }

    collie::Status SequentialWriteFile::write(const void *data, size_t size) {
        INVALID_FD_RETURN(_fd);
        auto rs = sys_write(_fd, data, size);
        if (rs < 0) {
            return collie::Status::from_errno(errno, "write file {} failed", _file_path.c_str());
        }
        return collie::Status::ok_status();
    }

    collie::Result<size_t> SequentialWriteFile::size() const {
        INVALID_FD_RETURN(_fd);
        auto rs = file_size(_fd);
        if(rs < 0) {
            return collie::Status::from_errno(errno, "get file size failed");
        }
        return rs;
    }

    void SequentialWriteFile::close() {
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

    collie::Status SequentialWriteFile::truncate(size_t size) {
        INVALID_FD_RETURN(_fd);
        if (::ftruncate(_fd, static_cast<off_t>(size)) != 0) {
            return collie::Status::from_errno(errno, "Failed truncate file {} for size:{} ", _file_path.c_str(),
                                              static_cast<off_t>(size));
        }
        if(::lseek(_fd, static_cast<off_t>(size), SEEK_SET) != 0) {
            return collie::Status::from_errno(errno, "Failed seek file end {} for size:{} ", _file_path.c_str(),
                                              static_cast<off_t>(size));
        }
        return collie::Status::ok_status();
    }

    collie::Status SequentialWriteFile::flush() {
        INVALID_FD_RETURN(_fd);
        if (::fsync(_fd) != 0) {
            return collie::Status::from_errno(errno,
                                              collie::format("Failed flush to file {}", _file_path.c_str()));
        }
        return collie::Status::ok_status();
    }

    const collie::filesystem::path &SequentialWriteFile::file_path() const {
        return _file_path;
    }

}  // namespace alkaid
