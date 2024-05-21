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

#ifdef _WIN32
# include <windows.h>
#else

# include <unistd.h>

#endif

namespace alkaid {

    /**
     * This is used by `basic_mmap` to determine whether to create a read-only or
     * a read-write memory mapping.
     */
    enum class access_mode {
        read,
        write
    };

    /**
     * Determines the operating system's page allocation granularity.
     *
     * On the first call to this function, it invokes the operating system specific syscall
     * to determine the page size, caches the value, and returns it. Any subsequent call to
     * this function serves the cached value, so no further syscalls are made.
     */
    inline size_t page_size() {
        static const size_t page_size = [] {
#ifdef _WIN32
            SYSTEM_INFO SystemInfo;
            GetSystemInfo(&SystemInfo);
            return SystemInfo.dwAllocationGranularity;
#else
            return sysconf(_SC_PAGE_SIZE);
#endif
        }();
        return page_size;
    }

    /**
     * Alligns `offset` to the operating's system page size such that it subtracts the
     * difference until the nearest page boundary before `offset`, or does nothing if
     * `offset` is already page aligned.
     */
    inline size_t make_offset_page_aligned(size_t offset) noexcept {
        const size_t page_size_ = page_size();
        // Use integer division to round down to the nearest page alignment.
        return offset / page_size_ * page_size_;
    }

} // namespace alkaid
