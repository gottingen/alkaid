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
#include <alkaid/files/local/defines.h>

namespace alkaid::lfs {
    class SequentialReadFile : public SequentialFileReader {
    public:
        SequentialReadFile() = default;

        ~SequentialReadFile() override;

        turbo::Status open(const std::string &path, std::any options, FileEventListener listener) noexcept override;

        turbo::Status close() noexcept override {
            return close_impl();
        }

        turbo::Result<int64_t> tell() const noexcept override;

        FileMode mode() const noexcept override {
            return FileMode::READ;
        }

        const std::string &path() const noexcept override {
            return path_;
        }

        turbo::Status advance(off_t n) noexcept override;

        turbo::Result<size_t> size() const noexcept override;

    private:
        turbo::Result<size_t> read_impl(void *buff, size_t len) noexcept override;

        turbo::Status close_impl() noexcept;


    private:
        FILE_HANDLER _fd{INVALID_FILE_HANDLER};
        std::string path_;
        OpenOption open_option_{kDefaultReadOption};
        FileEventListener listener_;
    };
}  // namespace alkaid::lfs

