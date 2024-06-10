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
// Created by jeff on 24-6-10.
//

#pragma once

#include <alkaid/files/filesystem.h>
#include <turbo/strings/cord.h>
#include <turbo/utility/status.h>
#include <turbo/base/endian.h>
#include <string>

namespace alkaid {

    class BufferedWriter {
    public:
        static constexpr size_t kDefaultCacheSize = 1024 * 1024;
    public:
        // just take a copy of the writer, do not take the ownership
        BufferedWriter(const std::shared_ptr<SequentialFileWriter> &writer, size_t cache_size = kDefaultCacheSize);

        ~BufferedWriter();

        turbo::Status write(const turbo::Cord &cord);

        turbo::Status write(const std::string &str);

        turbo::Status write(const turbo::Nonnull<const uint8_t *> data, size_t size);

        template<typename T>
        turbo::Status write_type(const T &value);

        turbo::Status write_char(char value);

        turbo::Status write_uchar(unsigned char value);

        template<bool big_endian = false>
        turbo::Status write_int16(int16_t value);

        template<bool big_endian = false>
        turbo::Status write_uint16(uint16_t value);

        template<bool big_endian = false>
        turbo::Status write_int32(int32_t value);

        template<bool big_endian = false>
        turbo::Status write_uint32(uint32_t value);

        template<bool big_endian = false>
        turbo::Status write_int64(int64_t value);

        template<bool big_endian = false>
        turbo::Status write_uint64(uint64_t value);

        template<bool big_endian = false>
        turbo::Status write_float(float value);

        template<bool big_endian = false>
        turbo::Status write_double(double value);

        turbo::Status write_bool(bool value);

        turbo::Status flush();

        // finalize the writer, flush the cache and close the writer
        // must be called before the writer is destructed
        turbo::Status finalize();

    private:
        turbo::Status flush_impl();

    private:
        size_t cache_size_;
        std::shared_ptr<SequentialFileWriter> writer_;
        turbo::Cord cache_;
        bool finalized_{false};
    };

    template<typename T>
    inline turbo::Status BufferedWriter::write_type(const T &value) {
        return write(reinterpret_cast<const uint8_t *>(&value), sizeof(T));
    }

    inline turbo::Status BufferedWriter::write_char(char value) {
        return write_type(value);
    }

    inline turbo::Status BufferedWriter::write_uchar(unsigned char value) {
        return write_type(value);
    }

    template<bool big_endian>
    inline turbo::Status BufferedWriter::write_int16(int16_t value) {
        if constexpr (big_endian) {
            auto wvalue = turbo::ghtons(value);
            return write_type(wvalue);
        }
        return write_type(value);
    }

    template<bool big_endian>
    inline turbo::Status BufferedWriter::write_uint16(uint16_t value) {
        if constexpr (big_endian) {
            auto wvalue = turbo::ghtons(value);
            return write_type(wvalue);
        }
        return write_type(value);
    }

    template<bool big_endian>
    inline turbo::Status BufferedWriter::write_int32(int32_t value) {
        if constexpr (big_endian) {
            auto wvalue = turbo::ghtonl(value);
            return write_type(wvalue);
        }
        return write_type(value);

    }

    template<bool big_endian>
    inline turbo::Status BufferedWriter::write_uint32(uint32_t value) {
        if constexpr (big_endian) {
            auto wvalue = turbo::ghtonl(value);
            return write_type(wvalue);
        }
        return write_type(value);
    }

    template<bool big_endian>
    inline turbo::Status BufferedWriter::write_int64(int64_t value) {
        if constexpr (big_endian) {
            auto wvalue = turbo::ghtonll(value);
            return write_type(wvalue);
        }
        return write_type(value);
    }

    template<bool big_endian>
    inline turbo::Status BufferedWriter::write_uint64(uint64_t value) {
        if constexpr (big_endian) {
            auto wvalue = turbo::ghtonll(value);
            return write_type(wvalue);
        }
        return write_type(value);
    }

    template<bool big_endian>
    inline turbo::Status BufferedWriter::write_float(float value) {
        if constexpr (big_endian) {
            if constexpr (sizeof(float) == 4) {
                auto wvalue = turbo::ghtonl(static_cast<uint32_t>(value));
                return write_type(wvalue);
            } else if constexpr (sizeof(float) == 8) {
                auto wvalue = turbo::ghtonll(static_cast<uint64_t>(value));
                return write_type(wvalue);
            } else {
                return turbo::data_loss_error("unsupported float size");
            }
        }
        return write_type(value);
    }

    template<bool big_endian>
    inline turbo::Status BufferedWriter::write_double(double value) {
        if constexpr (big_endian) {
            if constexpr (sizeof(double) == 4) {
                auto wvalue = turbo::ghtonl(static_cast<uint32_t>(value));
                return write_type(wvalue);
            } else if constexpr (sizeof(double) == 8) {
                auto wvalue = turbo::ghtonll(static_cast<uint64_t>(value));
                return write_type(wvalue);
            } else {
                return turbo::data_loss_error("unsupported double size");
            }
        }
        return write_type(value);
    }

    inline turbo::Status BufferedWriter::write_bool(bool value) {
        return write_type(value);
    }
}  // namespace alkaid
