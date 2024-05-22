// Copyright 2023 The Turbo Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once

#include <string>
#include <cstdio>
#include <tuple>
#include <collie/utility/result.h>

namespace alkaid {

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
        static collie::Status list_files(const std::string_view &root_path,std::vector<std::string> &result, bool full_path = true) noexcept;

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
        static collie::Status list_directories(const std::string_view &root_path,std::vector<std::string> &result, bool full_path = true) noexcept;

        /**
         * @ingroup turbo_files_utility
         * @brief read file content.
         * @param file_path [input] file path
         * @param result [output] file content
         * @param append [input] if true, the file content will be appended to the result.
         * @return the status of the operation.
         */
        static collie::Status read_file(const std::string_view &file_path, std::string &result, bool append = false) noexcept;

        /**
         * @ingroup turbo_files_utility
         * @brief write file content.
         * @param file_path [input] file path
         * @param content [input] file content
         * @param truncate [input] if true, the file will be truncated before write.
         * @return the status of the operation.
         */
        static collie::Status write_file(const std::string_view &file_path, const std::string_view &content, bool truncate = true) noexcept;
}  // namespace alkaid