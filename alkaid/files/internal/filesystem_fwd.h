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

#pragma once


#include <string_view>
#include <any>
#include <string>
#include <alkaid/files/interface.h>
#include <turbo/strings/cord.h>
#include <turbo/utility/status.h>
#include <turbo/base/macros.h>
#include <alkaid/files/ghc/filesystem.hpp>

namespace alkaid {

    class LocalFilesystem;

    enum class CopyOptions : uint16_t {
        None = 0,
        SkipExisting = 1,
        OverwriteExisting = 2,
        OverwriteExistingIfNewer = 4,
        Recursive = 8,
        CopySymlinks = 0x10,
        SkipSymlinks = 0x20,
        DirectoriesOnly = 0x40,
        CreateSymlinks = 0x80,
    };

    class TURBO_EXPORT Filesystem {
    public:

        // get the local filesystem instance
        static LocalFilesystem *localfs();

        virtual ~Filesystem() = default;

        virtual std::string name() const = 0;

        virtual turbo::Result<std::shared_ptr<SequentialFileReader>> create_sequential_read_file() = 0;

        // only localfs support mmap
        virtual turbo::Result<std::shared_ptr<SequentialFileReader>> create_sequential_read_mmap_file() {
            return turbo::unimplemented_error("not implemented");
        }

        virtual turbo::Result<std::shared_ptr<RandomAccessFileReader>> create_random_read_file() = 0;

        // only localfs support mmap
        virtual turbo::Result<std::shared_ptr<RandomAccessFileReader>> create_random_read_mmap_file() {
            return turbo::unimplemented_error("not implemented");
        }

        virtual turbo::Result<std::shared_ptr<SequentialFileWriter>> create_sequential_write_file() = 0;

        virtual turbo::Result<std::shared_ptr<RandomAccessFileWriter>> create_random_write_file() = 0;

        virtual turbo::Result<std::shared_ptr<TempFileWriter>> create_temp_file() = 0;

        virtual turbo::Status read_file(const std::string &file_path, std::string *result) noexcept = 0;

        virtual turbo::Status write_file(const std::string &file_path, const std::string_view &content) noexcept = 0;

        virtual turbo::Status append_file(const std::string &file_path, const std::string_view &content) noexcept = 0;

        /**
     * @ingroup turbo_files_utility
     * @brief list files in the specified directory.
     *        If the directory does not exist, it will be returned with an error.
     *        If the directory is not a directory, it will be returned with an error.
     *        If the full_path is true, the result will be the full path of the file.
     *        eg : root_path = "/tmp", full_path = true, result = ["/tmp/file1", "/tmp/file2"]
     *        if the full_path is false, the result will be the file name.
     *        eg : root_path = "/tmp", full_path = false, result = ["file1", "file2"]
     * @param root_path directory path
     * @param result [output] file list
     * @param full_path [input] if true, the result will be the full path of the file.
     * @return the status of the operation.
     */
        virtual turbo::Status list_files(const std::string_view &root_path, std::vector<std::string> &result,
                                         bool full_path = true) noexcept = 0;

        /**
         * @ingroup turbo_files_utility
         * @brief list directories in the specified directory.
         *        If the directory does not exist, it will be returned with an error.
         *        If the directory is not a directory, it will be returned with an error.
         *        If the full_path is true, the result will be the full path of the directory.
         *        eg : root_path = "/tmp", full_path = true, result = ["/tmp/dir1", "/tmp/dir2"]
         *        if the full_path is false, the result will be the directory name.
         *        eg : root_path = "/tmp", full_path = false, result = ["dir1", "dir2"]
         * @param root_path directory path
         * @param result [output] directory list
         * @param full_path [input] if true, the result will be the full path of the directory.
         * @return the status of the operation.
         */
        virtual turbo::Status list_directories(const std::string_view &root_path, std::vector<std::string> &result,
                                               bool full_path = true) noexcept = 0;

        virtual turbo::Result<bool> exists(const std::string_view &path) noexcept = 0;

        virtual turbo::Status remove(const std::string_view &path) noexcept = 0;

        virtual turbo::Status remove_all(const std::string_view &path) noexcept = 0;

        virtual turbo::Status remove_if_exists(const std::string_view &path) noexcept = 0;

        virtual turbo::Status remove_all_if_exists(const std::string_view &path) noexcept = 0;

        virtual turbo::Result<size_t> file_size(const std::string_view &path) noexcept = 0;

        virtual turbo::Result<turbo::Time> last_modified_time(const std::string_view &path) noexcept = 0;

        virtual turbo::Status rename(const std::string_view &old_path, const std::string_view &new_path) noexcept = 0;

        virtual turbo::Status copy_file(const std::string_view &src_path, const std::string_view &dst_path) noexcept = 0;

        virtual turbo::Status file_resize(const std::string_view &path, size_t size) noexcept = 0;

        virtual turbo::Result<std::string> temp_directory_path() noexcept = 0;

        virtual turbo::Status create_directory(const std::string_view &path) noexcept = 0;

        virtual turbo::Status create_directories(const std::string_view &path) noexcept = 0;

        // copy with options
        virtual turbo::Status copy_directory(const std::string_view &src_path, const std::string_view &dst_path, CopyOptions options) noexcept = 0;

        // only copy dir and files in the directory
        virtual turbo::Status copy_directory(const std::string_view &src_path, const std::string_view &dst_path) noexcept {
            auto options = CopyOptions::None;
            return copy_directory(src_path, dst_path, options);
        }
        virtual turbo::Status copy_directories(const std::string_view &src_path, const std::string_view &dst_path) noexcept {
            auto options = CopyOptions::Recursive;
            return copy_directory(src_path, dst_path, options);
        }
    };
}  // namespace alkaid
