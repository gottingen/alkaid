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

#pragma once

#include <turbo/utility/status.h>
#include <alkaid/files/internal/filesystem.h>
#include <turbo/crypto/md5.h>
#include <turbo/crypto/crc32c.h>

namespace alkaid {

    turbo::Status read_file(const FilePath &file_path, std::string *result) noexcept;

    turbo::Status write_file(const FilePath &file_path, const std::string_view &content) noexcept;

    turbo::Status append_file(const FilePath &file_path, const std::string_view &content) noexcept;

    turbo::Status list_files(const FilePath &root_path, std::vector<std::string> &result, bool full_path) noexcept;

    turbo::Status list_directories(const FilePath &root_path, std::vector<std::string> &result, bool full_path) noexcept;

    turbo::Result<uint32_t> crc32csum_file(const FilePath &file_path, size_t block_size = 4096) noexcept;

    using MD5Result = turbo::MD5::MD5Result;
    turbo::Result<MD5Result> md5sum_file(const FilePath &file_path, size_t block_size = 4096) noexcept;


}  // namespace alkaid
