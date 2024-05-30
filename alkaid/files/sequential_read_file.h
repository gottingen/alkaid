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

    class SequentialReadFile : public SequentialFileReader {
    public:
        SequentialReadFile() = default;

        explicit SequentialReadFile(const FileEventListener &listener);

        ~SequentialReadFile() override;

        SequentialReadFile(const SequentialReadFile &) = delete;
        SequentialReadFile &operator=(const SequentialReadFile &) = delete;

        /**
         * @brief open file with path and option specified by user.
         *        If the file does not exist, it will be returned with an error.
         * @param path file path
         * @param option file option
         */
        [[nodiscard]] turbo::Status open(const ghc::filesystem::path &path, const OpenOption &option = kDefaultReadOption) noexcept override;

        /**
         * @brief read file content sequentially from the current position to the specified length.
         * @param content [output] file content, can not be nullptr.
         * @param n [input] read length, default is npos, which means read all. If the length is greater than the file
         *          size, the file content will be read from the current position to the end of the file.
         * @return the length of the file content read and the status of the operation.
         */
        [[nodiscard]] turbo::Result<size_t> read(std::string *content, size_t n = kInfiniteFileSize) override;

        /**
         * @brief read file content sequentially from the current position to the specified length.
         * @param buff [output] file content, can not be nullptr.
         * @param len [input] read length, The length must be less than or equal to the size of the buff.
         *        If the length is greater than the file size, the file content will be read from the current position to the end of the file.
         *        If the length is less than the file size, the file content will be read from the current position to the length.
         * @return the length of the file content read and the status of the operation.
         */
        [[nodiscard]] turbo::Result<size_t> read(void *buff, size_t len) override;

        /**
         * @brief skip file descriptor sequentially from the current position to the position specified by offset.
         *        after skip, the current position will be offset + current position.
         * @param n [input] skip length, if n + current position is greater than the file size, the current position will be set to the end of the file.
         * @return the status of the operation.
         */
        [[nodiscard]] turbo::Status skip(off_t n) override;

        /**
         * @brief if the current position is the end of the file, return true, otherwise return false.
         * @return the status of the operation.
         */
        turbo::Result<bool> is_eof() const override;

        /**
         * @brief close file.
         */
        void close() override;

        size_t position() const override { return _position; }

        /**
         * @brief get file path.
         * @return file path.
         */
        [[nodiscard]] const ghc::filesystem::path &path() const { return _file_path; }

    private:
        FILE_HANDLER _fd{INVALID_FILE_HANDLER};
        ghc::filesystem::path _file_path;
        OpenOption _option;
        FileEventListener _listener;
        size_t _position{0};
    };

}  // namespace alkaid
