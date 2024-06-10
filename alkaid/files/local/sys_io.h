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

#pragma once

#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>

#include <alkaid/files/local/defines.h>

namespace alkaid::lfs {

    ssize_t sys_pwritev(FILE_HANDLER fd, const struct iovec *vector, int count, off_t offset);

    ssize_t sys_preadv(FILE_HANDLER fd, const struct iovec *vector, int count, off_t offset);


    ssize_t sys_pwrite(FILE_HANDLER fd, const void *data, int count, off_t offset);

    ssize_t sys_pread(FILE_HANDLER fd, const void *data, int count, off_t offset);

    ssize_t sys_writev(FILE_HANDLER fd, const struct iovec *vector, int count);

    ssize_t sys_readv(FILE_HANDLER fd, const struct iovec *vector, int count);

    ssize_t sys_write(FILE_HANDLER fd, const void *data, int count);

    ssize_t sys_read(FILE_HANDLER fd, const void *data, int count);

    ssize_t file_size(FILE_HANDLER fd);

    turbo::Result<FILE_HANDLER> open_file(const std::string &filename, const OpenOption &option);

    turbo::Result<FILE_HANDLER> open_file_read(const std::string &filename);

    turbo::Result<FILE_HANDLER> open_file_write(const std::string &filename, bool truncate = false);
}  // namespace alkaid::lfs
