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

#pragma once

#include <array>
#if defined(_WIN32)
# ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
# endif
# include <Windows.h>
# undef max
# undef min
#elif defined(__linux__)
# include <unistd.h>
#endif
namespace alkaid {
    static constexpr int CSV_NOT_FOUND = -1;

    namespace internals {
        // PAGE_SIZE macro could be already defined by the host system.
#if defined(PAGE_SIZE)
#undef PAGE_SIZE
#endif

// Get operating system specific details
#if defined(_WIN32)
        inline int getpagesize() {
            _SYSTEM_INFO sys_info = {};
            GetSystemInfo(&sys_info);
            return std::max(sys_info.dwPageSize, sys_info.dwAllocationGranularity);
        }

        const int PAGE_SIZE = getpagesize();
#elif defined(__linux__)
        const int PAGE_SIZE = getpagesize();
#else
        /** Size of a memory page in bytes. Used by
         *  csv::internals::CSVFieldArray when allocating blocks.
         */
        const int PAGE_SIZE = 4096;
#endif

        /** For functions that lazy load a large CSV, this determines how
         *  many bytes are read at a time
         */
        constexpr size_t ITERATION_CHUNK_SIZE = 10000000; // 10MB

        template<typename T>
        inline bool is_equal(T a, T b, T epsilon = 0.001) {
            /** Returns true if two floating point values are about the same */
            static_assert(std::is_floating_point<T>::value, "T must be a floating point type.");
            return std::abs(a - b) < epsilon;
        }

        enum class ParseFlags {
            QUOTE_ESCAPE_QUOTE = 0, /**< A quote inside or terminating a quote_escaped field */
            QUOTE = 2 | 1,          /**< Characters which may signify a quote escape */
            NOT_SPECIAL = 4,        /**< Characters with no special meaning or escaped delimiters and newlines */
            DELIMITER = 4 | 2,      /**< Characters which signify a new field */
            NEWLINE = 4 | 2 | 1     /**< Characters which signify a new row */
        };

        /** Transform the ParseFlags given the context of whether or not the current
         *  field is quote escaped */
        constexpr ParseFlags quote_escape_flag(ParseFlags flag, bool quote_escape) noexcept {
            return (ParseFlags)((int)flag & ~((int)ParseFlags::QUOTE * quote_escape));
        }

        /** An array which maps ASCII chars to a parsing flag */
        using ParseFlagMap = std::array<ParseFlags, 256>;

        /** An array which maps ASCII chars to a flag indicating if it is whitespace */
        using WhitespaceMap = std::array<bool, 256>;
    }
}  // namespace alkaid
