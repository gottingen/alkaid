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
#include <alkaid/files/filesystem_fwd.h>
#include <alkaid/files/local/defines.h>

namespace alkaid {


    struct LocalFilesystemOptions {
        bool use_mmap{false};
    };

    class TURBO_EXPORT LocalFilesystem : public Filesystem {
    public:
        LocalFilesystem() = default;

        std::string name() const override {
            return "LocalFilesystem";
        }

        turbo::Result<std::shared_ptr<SequentialFileReader>> create_sequential_read_file() override;

        turbo::Result<std::shared_ptr<SequentialFileReader>> create_sequential_read_mmap_file() override;

        turbo::Result<std::shared_ptr<RandomAccessFileReader>> create_random_read_file() override;

        turbo::Result<std::shared_ptr<RandomAccessFileReader>> create_random_read_mmap_file() override;

        turbo::Result<std::shared_ptr<SequentialFileWriter>> create_sequential_write_file() override;

        turbo::Result<std::shared_ptr<RandomAccessFileWriter>> create_random_write_file() override;

        turbo::Result<std::shared_ptr<TempFileWriter>> create_temp_file() override;

        turbo::Status read_file(const std::string &file_path, std::string *result) noexcept override;

        turbo::Status write_file(const std::string &file_path, const std::string_view &content) noexcept override;

        turbo::Status append_file(const std::string &file_path, const std::string_view &content) noexcept override;

        turbo::Status list_files(const std::string_view &root_path, std::vector<std::string> &result,
                                 bool full_path = true) noexcept override;

        turbo::Status list_directories(const std::string_view &root_path, std::vector<std::string> &result,
                                       bool full_path = true) noexcept override;

        turbo::Result<bool> exists(const std::string_view &path) noexcept override;

        turbo::Status remove(const std::string_view &path) noexcept override;

        turbo::Status remove_all(const std::string_view &path) noexcept override;

        turbo::Status remove_if_exists(const std::string_view &path) noexcept override;

        turbo::Status remove_all_if_exists(const std::string_view &path) noexcept override;

        turbo::Result<size_t> file_size(const std::string_view &path) noexcept override;

        turbo::Status file_resize(const std::string_view &path, size_t size) noexcept override;

        turbo::Result<turbo::Time> last_modified_time(const std::string_view &path) noexcept override;

        turbo::Status rename(const std::string_view &old_path, const std::string_view &new_path) noexcept override;

        turbo::Status copy_file(const std::string_view &src_path, const std::string_view &dst_path) noexcept override;

        turbo::Result<std::string> temp_directory_path() noexcept override;

        turbo::Status create_directory(const std::string_view &path) noexcept override;

        turbo::Status create_directories(const std::string_view &path) noexcept override;

        turbo::Status copy_directory(const std::string_view &src_path, const std::string_view &dst_path, CopyOptions options) noexcept override;

    };

}  // namespace alkaid
