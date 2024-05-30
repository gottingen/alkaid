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

#include <alkaid/files/fwd.h>

namespace alkaid {

    /**
     * @ingroup turbo_files_write_file
     * @brief SequentialWriteFile is a file io utility class.
     * eg.
     * @code
     *     SequentialWriteFile file;
     *     auto rs = file.open("test.txt");
     *     if (!rs.ok()) {
     *         std::cout << rs.status().message() << std::endl;
     *         return;
     *     }
     *     std::string content = "hello world";
     *     // write content to file.
     *     rs = file.write(content);
     *     if (!rs.ok()) {
     *         std::cout << rs.status().message() << std::endl;
     *         return;
     *     }
     *     // flush file.
     *     rs = file.flush();
     *     if (!rs.ok()) {
     *         std::cout << rs.status().message() << std::endl;
     *         return;
     *      }
     *      // close file or use RAII, it is not necessary to call flush before close.
     *      // and recommend to use close manually.
     *      file.close();
     *      // or use RAII.
     * @endcode
     */
    class SequentialWriteFile : public SequentialFileWriter {
    public:
        SequentialWriteFile() = default;

        /**
         * @brief set_option set file option before open file.
         *        default option is FileOption::kDefault.
         *        If you want to set the file option, you must call this function before open file.
         */
        explicit SequentialWriteFile(const FileEventListener &listener);

        ~SequentialWriteFile();

        /**
         * @brief open file with path and option specified by user.
         *        The option can be set by set_option function. @see set_option.
         *        If the file does not exist, it will be created.
         *        If the file exists, it will be opened.
         *        If the file exists and the truncate option is true, the file will be truncated.
         * @param fname file path
         * @param truncate if true, the file will be truncated.
         * @return the status of the operation.
         */

        [[nodiscard]] turbo::Status open(const ghc::filesystem::path &fname, const OpenOption &option) noexcept override;

        /**
         * @brief reopen file with path and option specified by user.
         *        The option can be set by set_option function. @see set_option.
         *        If the file does not exist, it will be created.
         *        If the file exists, it will be opened.
         *        If the file exists and the truncate option is true, the file will be truncated.
         * @param fname file path
         * @param truncate if true, the file will be truncated.
         * @return the status of the operation.
         */
        [[nodiscard]] turbo::Status reopen(bool truncate = false);

        /**
         * @brief write file content to the end of the file.
         * @param data [input] file content, can not be nullptr.
         * @param size [input] write length.
         * @return the status of the operation.
         */
        [[nodiscard]] turbo::Status write(const void *buff, size_t size) override;

        /**
         * @brief write file content to the end of the file.
         * @param str [input] file content, can not be empty.
         * @return the status of the operation.
         */
        [[nodiscard]] turbo::Status write(std::string_view str) override {
            return write(str.data(), str.size());
        }

        /**
         * @brief truncate file to the specified length.
         * @param size [input] file length.
         * @return the status of the operation.
         */
        [[nodiscard]] turbo::Status truncate(size_t size) override;

        /**
         * @brief get file size.
         * @return the file size and the status of the operation.
         */
        [[nodiscard]] turbo::Result<size_t> size() const override;

        /**
         * @brief close file.
         */
        void close() override;

        /**
         * @brief flush file.
         */
        [[nodiscard]]
        turbo::Status flush() override;

        /**
         * @brief get file path.
         * @return file path.
         */
        [[nodiscard]] const ghc::filesystem::path &file_path() const;

    private:
        static const size_t npos = std::numeric_limits<size_t>::max();
        FILE_HANDLER  _fd{INVALID_FILE_HANDLER};
        ghc::filesystem::path _file_path;
        OpenOption _option;
        FileEventListener _listener;
    };
} // namespace alkaid
