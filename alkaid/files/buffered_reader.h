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

    class BufferedReader {
    public:
        static constexpr size_t kDefaultCacheSize = 1024 * 1024;
    public:
        // just take a copy of the reader, do not take the ownership
        BufferedReader(const std::shared_ptr<SequentialFileReader> &reader, size_t cache_size = kDefaultCacheSize);

        // append result to the end of the cord
        turbo::Result<size_t> read(size_t size, turbo::Nonnull<turbo::Cord*> result);

        // append result to the end of the string
        turbo::Result<size_t> read(size_t size,  turbo::Nonnull<std::string *> result);

        // read to the buffer
        turbo::Result<size_t> read(size_t size, turbo::Nonnull<uint8_t*> result);

        bool reach_end() const {
            return reach_end_;
        }

        // read integral or floating point type
        // take attention to the endian
        template<typename T>
        turbo::Result<T> read_type();

        turbo::Result<char> read_char();

        turbo::Result<unsigned char> read_uchar();

        template<bool big_endian = false>
        turbo::Result<int16_t> read_int16();

        template<bool big_endian = false>
        turbo::Result<uint16_t> read_uint16();

        template<bool big_endian = false>
        turbo::Result<int32_t> read_int32();

        template<bool big_endian = false>
        turbo::Result<uint32_t> read_uint32();

        template<bool big_endian = false>
        turbo::Result<int64_t> read_int64();

        template<bool big_endian = false>
        turbo::Result<uint64_t> read_uint64();

        template<bool big_endian = false>
        turbo::Result<float> read_float();

        template<bool big_endian = false>
        turbo::Result<double> read_double();

        turbo::Result<bool> read_bool();

        void set_cache_size(size_t size);

        size_t cache_size() const {
            return cache_size_;
        }

    private:
        turbo::Result<size_t> fill_buffer(size_t size);
    private:
        bool reach_end_{false};
        size_t cache_size_;
        turbo::Cord cache_;
        std::shared_ptr<SequentialFileReader> reader_;
    };

    template<typename T>
    inline turbo::Result<T> BufferedReader::read_type() {
        static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>, "T must be integral or floating point type");
        T c;
        auto ret = read(sizeof(T), reinterpret_cast<uint8_t*>(&c));
        if (ret.ok()){
            if(ret.value() == sizeof(T)) {
                return c;
            }
            return turbo::data_loss_error("not enough data to read the type");
        }
        return ret.status();
    }

    inline turbo::Result<char> BufferedReader::read_char() {
        return read_type<char>();
    }

    inline turbo::Result<unsigned char> BufferedReader::read_uchar() {
        return read_type<unsigned char>();
    }

    template<bool big_endian>
    inline turbo::Result<int16_t> BufferedReader::read_int16() {
        auto rs =read_type<int16_t>();
        if(!rs.ok()) {
            return rs;
        }
        if constexpr (big_endian) {
            return turbo::gntohs(rs.value());
        }
        return rs;
    }

    template<bool big_endian>
    inline turbo::Result<uint16_t> BufferedReader::read_uint16() {
        auto rs =read_type<uint16_t>();
        if(!rs.ok()) {
            return rs;
        }
        if constexpr (big_endian) {
            return turbo::gntohs(rs.value());
        }
        return rs;
    }

    template<bool big_endian>
    inline turbo::Result<int32_t> BufferedReader::read_int32() {
        auto rs =read_type<int32_t>();
        if(!rs.ok()) {
            return rs;
        }
        if constexpr (big_endian) {
            return turbo::gntohl(rs.value());
        }
        return rs;
    }

    template<bool big_endian>
    inline turbo::Result<uint32_t> BufferedReader::read_uint32() {
        auto rs =read_type<uint32_t>();
        if(!rs.ok()) {
            return rs;
        }
        if constexpr (big_endian) {
            return turbo::gntohl(rs.value());
        }
        return rs;
    }

    template<bool big_endian>
    inline turbo::Result<int64_t> BufferedReader::read_int64() {
        auto rs =read_type<int64_t>();
        if(!rs.ok()) {
            return rs;
        }
        if constexpr (big_endian) {
            return turbo::gntohll(rs.value());
        }
        return rs;
    }

    template<bool big_endian>
    inline turbo::Result<uint64_t> BufferedReader::read_uint64() {
        auto rs =read_type<uint64_t>();
        if(!rs.ok()) {
            return rs;
        }
        if constexpr (big_endian) {
            return turbo::gntohll(rs.value());
        }
        return rs;
    }

    template<bool big_endian>
    inline turbo::Result<float> BufferedReader::read_float() {
        auto rs =read_type<float>();
        if(!rs.ok()) {
            return rs;
        }
        if constexpr (big_endian) {
            if constexpr (sizeof(float ) == 4) {
                return turbo::gntohl(static_cast<uint32_t>(rs.value()));
            } else if constexpr (sizeof(float) == 8) {
                return turbo::gntohll(static_cast<uint64_t>(rs.value()));
            } else {
                return turbo::data_loss_error("unsupported float size");
            }
        }
        return rs;
    }

    template<bool big_endian>
    inline turbo::Result<double> BufferedReader::read_double() {
        auto rs =read_type<double>();
        if(!rs.ok()) {
            return rs;
        }
        if constexpr (big_endian) {
            if constexpr (sizeof(double) == 4) {
                return turbo::gntohl(static_cast<uint32_t>(rs.value()));
            } else if constexpr (sizeof(double) == 8) {
                return turbo::gntohll(static_cast<uint64_t>(rs.value()));
            } else {
                return turbo::data_loss_error("unsupported double size");
            }
        }
        return rs;
    }

    inline turbo::Result<bool> BufferedReader::read_bool() {
        return read_type<bool>();
    }
}  // namespace alkaid
