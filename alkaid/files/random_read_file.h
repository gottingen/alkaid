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
     * @ingroup turbo_files_read_file
     * @brief RandomReadFile is a file io utility class.
     * eg.
     * @code
     *    RandomReadFile file;
     *    auto rs = file.open("test.txt");
     *    if (!rs.ok()) {
     *        std::cout << rs.status().message() << std::endl;
     *        return;
     *        // or throw exception.
     *    }
     *    std::string content;
     *    // read all content.
     *    rs = file.read(0, &content);
     *    if (!rs.ok()) {
     *        std::cout << rs.status().message() << std::endl;
     *        // or throw exception.
     *        return;
     *    }
     *    std::cout << content << std::endl;
     *    // read 10 bytes from offset 10.
     *    rs = file.read(10, &content, 10);
     *    if (!rs.ok()) {
     *        std::cout << rs.status().message() << std::endl;
     *        // or throw exception.
     *        return;
     *    }
     *    std::cout << content << std::endl;
     *    // close file or use RAII.
     *    file.close();
     * @endcode
     */
    class RandomReadFile :public  RandomAccessFileReader {
    public:
        RandomReadFile() = default;

        explicit RandomReadFile(const FileEventListener &listener);

        ~RandomReadFile() override;

        RandomReadFile(const RandomReadFile &) = delete;
        RandomReadFile &operator=(const RandomReadFile &) = delete;

        /**
         * @brief open file with path and option specified by user.
         *        If the file does not exist, it will be returned with an error.
         * @param path file path
         * @param option file option
         */
        [[nodiscard]] collie::Status open(const collie::filesystem::path &path, const OpenOption &option = kDefaultReadOption) noexcept override;

        /**
         * @brief read file content from offset to the specified length.
         * @param offset [input] file offset
         * @param content [output] file content, can not be nullptr.
         * @param n [input] read length, default is npos, which means read all. If the length is greater than the file
         *          size, the file content will be read from offset to the end of the file.
         * @return the length of the file content read and the status of the operation.
         */
        [[nodiscard]] collie::Result<size_t> read(off_t offset, std::string *content, size_t n = kInfiniteFileSize) override;

        /**
         * @brief read file content from offset to the specified length.
         * @param offset [input] file offset
         * @param buff [output] file content, can not be nullptr.
         * @param len [input] read length, and buff size must be greater than len. if from offset to the end of the file
         *            is less than len, the file content will be read from offset to the end of the file. the size of read
         *            content is the minimum of len and the file size from offset to the end of the file and will be returned
         *            in the result.
         * @return the length of the file content read and the status of the operation.
         */
        [[nodiscard]] collie::Result<size_t> read(off_t offset, void *buff, size_t len) override;

        /**
         * @brief close file.
         */
        void close() override;

    private:

        int        _fd;
        collie::filesystem::path _file_path;
        OpenOption _option;
        FileEventListener _listener;
    };

} // namespace alkaid