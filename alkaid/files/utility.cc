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

#include <alkaid/files/utility.h>
#include <alkaid/files/sequential_read_file.h>
#include <alkaid/files/sequential_write_file.h>
#include <turbo/strings/substitute.h>

namespace alkaid {

    turbo::Status read_file(const FilePath &file_path, std::string *result) noexcept {
        SequentialReadFile file;
        auto rs = file.open(file_path, std::any{}, FileEventListener{});
        if (!rs.ok()) {
            return rs;
        }
        auto rsize = file.size();
        if (!rsize.ok()) {
            return rsize.status();
        }
        auto r = file.read(result, rsize.value());
        if (!r.ok()) {
            return r.status();
        }
        return turbo::OkStatus();
    }

    turbo::Status write_file(const FilePath &file_path, const std::string_view &content) noexcept {
        SequentialWriteFile file;
        auto rs = file.open(file_path, kDefaultTruncateWriteOption, FileEventListener{});
        if (!rs.ok()) {
            return rs;
        }

        rs = file.append(content);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status append_file(const FilePath &file_path, const std::string_view &content) noexcept {
        SequentialWriteFile file;
        auto rs = file.open(file_path, kDefaultAppendWriteOption, FileEventListener{});
        if (!rs.ok()) {
            return rs;
        }

        rs = file.append(content);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status list_files(const FilePath &root_path, std::vector<std::string> &result,
                                              bool full_path) noexcept {
        alkaid::DirectoryIterator itr(root_path);
        while(itr) {
            RESULT_ASSIGN_OR_RETURN(auto is_dir, itr->is_directory());
            if (!is_dir) {
                if (full_path) {
                    result.emplace_back(itr->path().string());
                } else {
                    result.emplace_back(itr->path().filename());
                }
            }
            ++itr;
        }
        return itr.status();
    }

    turbo::Status list_directories(const FilePath &root_path, std::vector<std::string> &result, bool full_path) noexcept {
        alkaid::DirectoryIterator itr(root_path);
        while(itr) {
            RESULT_ASSIGN_OR_RETURN(auto is_dir, itr->is_directory());
            if (is_dir) {
                if (full_path) {
                    result.emplace_back(itr->path().string());
                } else {
                    result.emplace_back(itr->path().filename());
                }
            }
            ++itr;
        }
        return itr.status();
    }

    turbo::Result<MD5Result> md5sum_file(const FilePath &file_path, size_t block_size) noexcept {
        std::string content;
        content.reserve(block_size);
        RESULT_ASSIGN_OR_RETURN(auto file_size, alkaid::file_size(file_path));
        size_t read_size = 0;
        SequentialReadFile file;
        auto rs = file.open(file_path, std::any{}, FileEventListener{});
        if (!rs.ok()) {
            return rs;
        }
        turbo::MD5 md5;
        while (read_size < file_size) {
            size_t read_block_size = std::min(block_size, file_size - read_size);
            auto ss = file.read(&content, read_block_size);
            if (!ss.ok()) {
                return ss.status();
            }
            md5.update(content);
            read_size += read_block_size;
        }
        return md5.final();
    }

    turbo::Result<uint32_t> crc32csum_file(const FilePath &file_path, size_t block_size) noexcept {
        std::string content;
        content.reserve(block_size);
        RESULT_ASSIGN_OR_RETURN(auto file_size, alkaid::file_size(file_path));
        size_t read_size = 0;
        SequentialReadFile file;
        auto rs = file.open(file_path, std::any{}, FileEventListener{});
        if (!rs.ok()) {
            return rs;
        }
        turbo::CRC32C crc32c(0);
        while (read_size < file_size) {
            size_t read_block_size = std::min(block_size, file_size - read_size);
            auto ss = file.read(&content, read_block_size);
            if (!ss.ok()) {
                return ss.status();
            }
            crc32c = turbo::extend_crc32c(crc32c, content);
            read_size += read_block_size;
        }
        return (uint32_t)crc32c;
    }

}  // namespace alkaid
