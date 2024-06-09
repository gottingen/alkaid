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

#include <turbo/utility/status.h>
#include <turbo/base/macros.h>
#include <string_view>
#include <any>
#include <turbo/strings/cord.h>

namespace alkaid {

    enum class FileMode {
        READ, WRITE, READWRITE
    };

    static constexpr size_t kInfiniteFileSize = std::numeric_limits<size_t>::max();

    struct IORange {
        int64_t offset{0};
        int64_t length{0};

        friend bool operator==(const IORange &left, const IORange &right) {
            return (left.offset == right.offset && left.length == right.length);
        }

        friend bool operator!=(const IORange &left, const IORange &right) {
            return !(left == right);
        }

        bool contains(const IORange &other) const {
            return (offset <= other.offset && offset + length >= other.offset + other.length);
        }
    };

    class FileInterface;

    struct FileEventListener {
        std::function<void(const FileInterface *fi)> before_open;
        std::function<void(const FileInterface *fi)> after_open;
        std::function<void(const FileInterface *fi)> before_close;
        std::function<void(const FileInterface *fi)> after_close;
    };

    class TURBO_EXPORT FileInterface {
    public:
        virtual ~FileInterface() = default;

        virtual turbo::Status open(const std::string &path, std::any options, FileEventListener listener) noexcept = 0;

        virtual turbo::Status open(const std::string &path, std::any options) noexcept {
            return open(path, options, FileEventListener());
        }

        virtual turbo::Status open(const std::string &path) noexcept {
            return open(path, std::any(), FileEventListener());
        }


        virtual turbo::Status close() noexcept = 0;

        virtual turbo::Result<int64_t> tell() const noexcept = 0;

        virtual FileMode mode() const noexcept = 0;

        virtual const std::string &path() const noexcept = 0;

        virtual turbo::Result<size_t> size() const noexcept = 0;

    protected:
        FileInterface() = default;

    };

    class SequentialFileReader : public FileInterface {
    public:

        virtual  ~SequentialFileReader() = default;

        virtual turbo::Status advance(off_t n) noexcept = 0;

        virtual turbo::Result<size_t> read(void *buff, size_t len) noexcept ;

        virtual turbo::Result<size_t> read(std::string *result, size_t len) noexcept;

        virtual turbo::Result<size_t> read(turbo::Cord *buffer, size_t size) noexcept;

    protected:
        SequentialFileReader() = default;

        virtual turbo::Result<size_t> read_impl(void *buff, size_t len) noexcept = 0;
    };

    class RandomAccessFileReader : public FileInterface {
    public:

        virtual  ~RandomAccessFileReader() = default;

        virtual turbo::Result<size_t> read_at(off_t offset, void *buff, size_t len);

        virtual turbo::Result<size_t> read_at(off_t offset, std::string *result, size_t len = kInfiniteFileSize);

        virtual turbo::Result<size_t> read_at(off_t offset, turbo::Cord &buffer, size_t size = kInfiniteFileSize);

    protected:
        RandomAccessFileReader() = default;

        virtual turbo::Result<size_t> read_at_impl(off_t offset, void *buff, size_t len) = 0;
    };

    class SequentialFileWriter : public FileInterface {
    public:

        virtual  ~SequentialFileWriter() = default;

        virtual turbo::Status flush();

        virtual turbo::Status append(const void *buff, size_t length, bool truncate = false);

        virtual turbo::Status append(std::string_view buff, bool truncate = false);

        virtual turbo::Status append(const turbo::Cord &buffer, bool truncate = false);

        virtual turbo::Status truncate(size_t size) noexcept = 0;

    protected:
        virtual turbo::Status append_impl(const void *buff, size_t len) = 0;
    };

    class RandomAccessFileWriter : public FileInterface {
    public:

        virtual ~RandomAccessFileWriter() = default;

        virtual turbo::Status write_at(off_t offset, const void *buff, size_t len);

        virtual turbo::Status write_at(off_t offset, std::string_view buff);

        virtual turbo::Status write_at(off_t offset, const turbo::Cord &buffer);

        virtual turbo::Status flush();

        virtual turbo::Status truncate(size_t size) noexcept = 0;

    private:

        virtual turbo::Status write_at_impl(off_t offset, const void *buff, size_t len) noexcept = 0;

    };

    class TempFileWriter : public SequentialFileWriter {
    };

}  // namespace alkaid
