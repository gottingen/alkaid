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
     * @brief RandomWriteFile is a file io utility class.
     * eg.
     * @code
     *      RandomWriteFile file;
     *      auto rs = file.open("test.txt");
     *      if (!rs.ok()) {
     *          std::cout << rs.status().message() << std::endl;
     *          // or throw exception.
     *          return;
     *      }
     *      std::string content = "hello world";
     *      // write content to file.
     *      rs = file.write(0, content);
     *      if (!rs.ok()) {
     *          std::cout << rs.status().message() << std::endl;
     *          // or throw exception.
     *          return;
     *      }
     *      // write content to file from offset 10.
     *      rs = file.write(10, content);
     *      if (!rs.ok()) {
     *          std::cout << rs.status().message() << std::endl;
     *          // or throw exception.
     *          return;
     *       }
     *       // write content to file from offset 10 and truncate file.
     *       rs = file.write(10, content, true);
     *       if (!rs.ok()) {
     *           std::cout << rs.status().message() << std::endl;
     *           // or throw exception.
     *           return;
     *       }
     *       // when write file recommend to call flush function.
     *       file.flush();
     *       // close file or use RAII,  it is recommended to call flush function before close file.
     *       file.close();
     * @endcode
     */
    class RandomWriteFile : public  RandomFileWriter {
    public:

        RandomWriteFile() = default;

        explicit RandomWriteFile(const FileEventListener &listener);

        ///

        ~RandomWriteFile() override;

        /**
         * @brief open file with path and option specified by user.
         *        The option can be set by set_option function. @see set_option.
         *        If the file does not exist, it will be created.
         *        If the file exists, it will be opened.
         *        If the file exists and the truncate option is true, the file will be truncated.
         * @param fname file path
         * @param truncate truncate file if true, default is false.
         * @return the status of the operation. If the file is opened successfully, the status is OK.
         */
        [[nodiscard]] collie::Status open(const collie::filesystem::path &path, const OpenOption &option) noexcept override;
        /**
         * @brief reopen file with path and option specified by user.
         *        The option can be set by set_option function. @see set_option.
         *        If the file does not exist, it will be created.
         *        If the file exists, it will be opened.
         *        If the file exists and the truncate option is true, the file will be truncated.
         * @param truncate truncate file if true, default is false.
         * @return the status of the operation. If the file is opened successfully, the status is OK.
         */
        [[nodiscard]] collie::Status reopen(bool truncate = false);

        /**
         * @brief write file content from offset to the specified length.
         * @param offset [input] file offset
         * @param data [input] file content, can not be nullptr.
         * @param size [input] write length.
         * @param truncate [input] truncate file if true, default is false.
         *        If set to true, the file will be truncated to the length + offset.
         * @return the status of the operation.
         */
        [[nodiscard]] collie::Status write(off_t offset,const void *data, size_t size, bool truncate = false) override;

        /**
         * @brief write file content from offset to the specified length.
         * @param offset [input] file offset
         * @param str [input] file content, can not be empty.
         * @param truncate [input] truncate file if true, default is false.
         *       If set to true, the file will be truncated to the str.size() + offset.
         * @return the status of the operation.
         */
        [[nodiscard]] collie::Status write(off_t offset, std::string_view str, bool truncate = false) override {
            return write(offset, str.data(), str.size(), truncate);
        }
        /**
         * @brief truncate file to the specified length.
         * @param size [input] file length.
         * @return the status of the operation.
         */
        [[nodiscard]] collie::Status truncate(size_t size) override;

        /**
         * @brief get file size.
         * @return the file size and the status of the operation.
         */
        [[nodiscard]] collie::Result<size_t> size() const override;

        /**
         * @brief close file.
         */
        void close() override;

        /**
         * @brief flush file.
         * @return the status of the operation.
         */
        [[nodiscard]]
        collie::Status flush() override;

        /**
         * @brief get file path.
         * @return file path.
         */
        [[nodiscard]] const collie::filesystem::path &file_path() const;

    private:
        static const size_t npos = std::numeric_limits<size_t>::max();
        int        _fd{-1};
        collie::filesystem::path _file_path;
        OpenOption _option;
        FileEventListener _listener;
    };
}  // namespace alkaid

