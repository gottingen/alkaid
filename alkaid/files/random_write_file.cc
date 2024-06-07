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
#include <alkaid/files/random_write_file.h>
#include <turbo/times/time.h>
#include <turbo/strings/substitute.h>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>

namespace alkaid {

    RandomWriteFile::RandomWriteFile(const FileEventListener &listener) : _listener(listener) {

    }

    RandomWriteFile::~RandomWriteFile() {
        close();
    }


    turbo::Status
    RandomWriteFile::open(const ghc::filesystem::path &fname, const OpenOption &option) noexcept {
        close();
        _option = option;
        _file_path = fname;
        if (_file_path.empty()) {
            return turbo::invalid_argument_error("file path is empty");
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
        return turbo::errno_to_status(errno, "Failed opening file {} for writing");
    }

    turbo::Status RandomWriteFile::reopen(bool truncate) {
        close();
        if (_file_path.empty()) {
            return turbo::invalid_argument_error("file path is empty");
        }
        OpenOption option = _option;
        if (truncate) {
            option.truncate();
        }
        return open(_file_path, option);
    }

    turbo::Status RandomWriteFile::write(off_t offset, const void *data, size_t size, bool truncate) {
        INVALID_FD_RETURN(_fd);

        ssize_t write_size = ::pwrite(_fd, data, size, static_cast<off_t>(offset));
        if (write_size < 0) {
            return turbo::errno_to_status(errno, _file_path.c_str());
        }
        if (truncate) {
            if (::ftruncate(_fd, static_cast<off_t>(offset + size)) != 0) {
                return turbo::errno_to_status(errno, turbo::substitute("Failed truncate file $0 for size:$1 ",
                                                                     _file_path.c_str(),
                                                                     static_cast<off_t>(offset + size)));
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status RandomWriteFile::truncate(size_t size) {
        if (::ftruncate(_fd, static_cast<off_t>(size)) != 0) {
            return turbo::errno_to_status(errno,
                                        turbo::substitute("Failed truncate file $0 for size:$1 ", _file_path.c_str(),
                                                          static_cast<off_t>(size)));
        }
        return turbo::OkStatus();
    }

    turbo::Result<size_t> RandomWriteFile::size() const {
        INVALID_FD_RETURN(_fd);
        auto rs = file_size(_fd);
        if (rs == -1) {
            return turbo::errno_to_status(errno, "get file size failed");
        }
        return rs;
    }

    void RandomWriteFile::close() {
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

    turbo::Status RandomWriteFile::flush() {
        INVALID_FD_RETURN(_fd);
        if (::fdatasync(_fd) != 0) {
            return turbo::errno_to_status(errno, turbo::substitute("Failed flush to file $0", _file_path.c_str()));
        }
        return turbo::OkStatus();
    }

    const ghc::filesystem::path &RandomWriteFile::file_path() const {
        return _file_path;
    }

}  // namespace alkaid
