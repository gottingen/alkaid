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
#include <alkaid/files/local/mmap.h>

namespace alkaid::lfs {

    class RandomReadMMapFile : public RandomAccessFileReader {
    public:
        RandomReadMMapFile() = default;

        ~RandomReadMMapFile() override;

        turbo::Status open(const std::string &filename, std::any options, FileEventListener listener) noexcept override;

        turbo::Status close() noexcept override;

        turbo::Result<int64_t> tell() const noexcept override;

        FileMode mode() const noexcept override { return FileMode::READ; }

        const std::string &path() const noexcept override { return path_; }


        turbo::Result<size_t> size() const noexcept override;

    private:
        turbo::Result<size_t> read_at_impl(int64_t offset, void *buff, size_t len) noexcept override;

        turbo::Status close_impl() noexcept;

    private:
        alkaid::ummap_source mmap_source_;
        std::string path_;
        OpenOption open_option_{kDefaultReadOption};
        FileEventListener listener_;

    };
}  // namespace alkaid::lfs
