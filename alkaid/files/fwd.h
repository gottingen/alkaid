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

#include <turbo/utility/status.h>
#include <alkaid/ghc/filesystem.hpp>

#define INVALID_FD_RETURN(fd) \
    if ((fd) == INVALID_FILE_HANDLER) { \
        return ::turbo::invalid_argument_error("file not open for read yet"); \
    }

namespace alkaid {

#if defined(__linux__)  || defined(__APPLE__)
    using FILE_HANDLER = int;
    static const FILE_HANDLER INVALID_FILE_HANDLER = -1;
#elif defined(_WIN32) || defined(_WIN64)
    using FILE_HANDLER = void*;
    static const FILE_HANDLER INVALID_FILE_HANDLER = nullptr;
#endif


    struct OpenOption {
        int32_t open_tries{1};
        uint32_t open_interval_ms{0};
        int flags{0};
        int mode{0644};
        bool create_dir_if_miss{false};

        constexpr OpenOption() = default;

        constexpr OpenOption(const OpenOption &) = default;

        constexpr OpenOption(OpenOption &&) = default;

        constexpr OpenOption &operator=(const OpenOption &) = default;

        constexpr OpenOption &operator=(OpenOption &&) = default;

        constexpr OpenOption &tries(int32_t tries) {
            open_tries = tries;
            return *this;
        }

        constexpr OpenOption &interval_ms(uint32_t interval) {
            open_interval_ms = interval;
            return *this;
        }

        constexpr OpenOption &read_only() {
            flags |= O_RDONLY;
            return *this;
        }

        constexpr OpenOption &write_only() {
            flags |= O_WRONLY;
            return *this;
        }

        constexpr OpenOption &read_write() {
            flags |= O_RDWR;
            return *this;
        }

        constexpr OpenOption &append(bool append = true) {
            append ? flags |= O_APPEND : flags &= ~O_APPEND;
            return *this;
        }

        constexpr OpenOption &truncate(bool truncate = true) {
            truncate ? flags |= O_TRUNC : flags &= ~O_TRUNC;
            return *this;
        }

        constexpr OpenOption &create(bool create = true) {
            create ? flags |= O_CREAT : flags &= ~O_CREAT;
            return *this;
        }

        constexpr OpenOption &cloexec(bool cloexec = true) {
            cloexec ? flags |= O_CLOEXEC : flags &= ~O_CLOEXEC;
            return *this;
        }

        constexpr OpenOption &flag(int flag) {
            this->flags = flag;
            return *this;
        }

        constexpr OpenOption &create_dir(bool create_dir = true) {
            this->create_dir_if_miss = create_dir;
            return *this;
        }
    };

    static constexpr OpenOption kDefaultReadOption = OpenOption{1, 0, O_RDONLY | O_CLOEXEC, 0644, false};
    static constexpr OpenOption kDefaultAppendWriteOption = OpenOption{1, 0, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC,
                                                                       0644, false};
    static constexpr OpenOption kDefaultTruncateWriteOption = OpenOption{1, 0, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
                                                                         0644, false};

    struct FileEventListener {
        FileEventListener()
                : before_open(nullptr), after_open(nullptr), before_close(nullptr), after_close(nullptr) {}

        std::function<void(const ghc::filesystem::path &filename)> before_open;
        std::function<void(const ghc::filesystem::path &filename, FILE_HANDLER handler)> after_open;
        std::function<void(const ghc::filesystem::path &filename, FILE_HANDLER file_stream)> before_close;
        std::function<void(const ghc::filesystem::path &filename)> after_close;
    };

    static constexpr size_t kInfiniteFileSize = std::numeric_limits<size_t>::max();

    class SequentialFileReader {
    public:

        virtual  ~SequentialFileReader()   = default;

        [[nodiscard]] virtual turbo::Status open(const ghc::filesystem::path &path, const OpenOption &option = kDefaultReadOption) noexcept = 0;

        [[nodiscard]] virtual turbo::Status skip(off_t n) = 0;

        [[nodiscard]] virtual turbo::Result<size_t> read(void *buff, size_t len) = 0;

        [[nodiscard]] virtual turbo::Result<size_t> read(std::string *result, size_t len = kInfiniteFileSize) {
            return turbo::unimplemented_error("SequentialFileReader::read(std::string *result, size_t len)");
        }

        virtual void close() = 0;

        virtual size_t position() const = 0;

        [[nodiscard]] virtual turbo::Result<bool> is_eof() const = 0;

    };

    class RandomAccessFileReader {
    public:

        virtual  ~RandomAccessFileReader()   = default;

        [[nodiscard]] virtual turbo::Status open(const ghc::filesystem::path &path, const OpenOption &option = kDefaultReadOption) noexcept = 0;

        [[nodiscard]] virtual turbo::Result<size_t> read(off_t offset, void *buff, size_t len) = 0;

        [[nodiscard]] virtual turbo::Result<size_t> read(off_t offset, std::string *result, size_t len = kInfiniteFileSize) {
            return turbo::unimplemented_error("SequentialFileReader::read(std::string *result, size_t len)");
        }

        virtual void close() = 0;
    };

    class SequentialFileWriter {
    public:

        virtual  ~SequentialFileWriter() = default;

        [[nodiscard]] virtual turbo::Status open(const ghc::filesystem::path &path,const OpenOption &option) noexcept = 0;

        [[nodiscard]] virtual  turbo::Status write(const void *buff, size_t len) = 0;

        [[nodiscard]] virtual turbo::Status flush() = 0;

        [[nodiscard]] virtual  turbo::Status write(std::string_view buff) {
            return turbo::unimplemented_error("SequentialFileReader::read(std::string *result, size_t len)");
        }

        [[nodiscard]] virtual turbo::Status truncate(size_t size) = 0;

        [[nodiscard]] virtual turbo::Result<size_t> size() const = 0;

        virtual void close() = 0;

    };

    class RandomFileWriter {
    public:

        virtual ~RandomFileWriter() = default;

        [[nodiscard]] virtual turbo::Status open(const ghc::filesystem::path &path, const OpenOption &option) noexcept = 0;

        [[nodiscard]] virtual  turbo::Status
        write(off_t offset, const void *buff, size_t len, bool truncate = false) = 0;

        [[nodiscard]] virtual turbo::Status flush() = 0;

        [[nodiscard]] virtual turbo::Status
        write(off_t offset, std::string_view buff, bool truncate = false) {
            return turbo::unimplemented_error("SequentialFileReader::read(std::string *result, size_t len)");
        }

        [[nodiscard]] virtual turbo::Status truncate(size_t size) = 0;

        virtual turbo::Result<size_t> size() const = 0;

        virtual void close() = 0;

    };

    static constexpr std::string_view kDefaultTempFilePrefix = "temp_file_";

    class TempFileWriter {
    public:

        virtual ~TempFileWriter() = default;

        [[nodiscard]] virtual turbo::Status open(std::string_view prefix = kDefaultTempFilePrefix, std::string_view ext ="", size_t bits = 6) noexcept = 0;

        [[nodiscard]] virtual turbo::Status write(const void *buff, size_t len) = 0;

        [[nodiscard]] virtual turbo::Status flush() = 0;

        [[nodiscard]] virtual turbo::Status truncate(size_t size) = 0;

        [[nodiscard]] virtual turbo::Status write(std::string_view buff) {
            return turbo::unimplemented_error("SequentialFileReader::read(std::string *result, size_t len)");
        }

        [[nodiscard]] virtual std::string path() const = 0;

        virtual turbo::Result<size_t> size() const = 0;

        virtual void close() = 0;
    };
}  // namespace alkaid
