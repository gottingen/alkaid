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
#include <alkaid/ghc/filesystem.hpp>

namespace alkaid::lfs {

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

}  // namespace alkaid::lfs

#define INVALID_FD_RETURN(fd) \
    if ((fd) == INVALID_FILE_HANDLER) \
        return ::turbo::invalid_argument_error("file not open for read yet")




