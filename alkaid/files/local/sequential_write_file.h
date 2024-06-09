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

    class SequentialWriteFile : public SequentialFileWriter {
    public:
        SequentialWriteFile() = default;

        ~SequentialWriteFile() override;

        turbo::Status open(const std::string &path, std::any options, FileEventListener listener) noexcept override;

        turbo::Status close() noexcept override {
            return close_impl();
        }

        turbo::Result<int64_t> tell() const noexcept override;

        FileMode mode() const noexcept override {
            return FileMode::WRITE;
        }

        const std::string &path() const noexcept override {
            return path_;
        }

        turbo::Result<size_t> size() const noexcept override;

        turbo::Status truncate(size_t size) noexcept override;

    private:
        turbo::Status append_impl(const void *buff, size_t len) noexcept override;

        turbo::Status close_impl() noexcept;

    private:
        FILE_HANDLER _fd{INVALID_FILE_HANDLER};
        std::string path_;
        FileEventListener listener_;
        OpenOption open_option_{kDefaultAppendWriteOption};
    };

}  // namespace alkaid::lfs
