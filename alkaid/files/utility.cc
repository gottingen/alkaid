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

    turbo::Status md5sum_file(const FilePath &file_path, std::string *result) noexcept {
        std::string content;
        STATUS_RETURN_IF_ERROR(read_file(file_path, &content));
        turbo::MD5 md5;
        md5.update(content);
        turbo::MD5::MD5Result md5_result = md5.final();
        *result = md5_result.digest();
        return turbo::OkStatus();
    }

    turbo::Status md5sum_file(const FilePath &file_path, MD5Result *result) noexcept {
        std::string content;
        STATUS_RETURN_IF_ERROR(read_file(file_path, &content));
        turbo::MD5 md5;
        md5.update(content);
        md5.final(*result);
        return turbo::OkStatus();
    }

    turbo::Result<uint32_t> crc32csum_file(const FilePath &file_path) noexcept {
        std::string content;
        STATUS_RETURN_IF_ERROR(read_file(file_path, &content));
        turbo::CRC32C crc32c = turbo::compute_crc32c(content);
        return (uint32_t)crc32c;
    }

}  // namespace alkaid
