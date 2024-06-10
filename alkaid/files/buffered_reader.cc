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

#include <alkaid/files/buffered_reader.h>
#include <turbo/log/logging.h>
namespace alkaid {
    BufferedReader::BufferedReader(const std::shared_ptr<SequentialFileReader> &reader, size_t cache_size)
            : cache_size_(cache_size), reader_(reader) {
        CHECK(reader_) << "reader is nullptr";
    }
    turbo::Result<size_t> BufferedReader::read(size_t size, turbo::Nonnull<turbo::Cord *> result) {
        if (!reach_end_) {
            auto rs = fill_buffer(size);
            if (!rs.ok()) {
                return rs.status();
            }
        }
        if (cache_.empty()) {
            return 0;
        }
        if (size >= cache_.size()) {
            result->append(std::move(cache_));
            cache_.clear();
            return cache_.size();
        }

        turbo::Cord tmp;
        tmp.append(cache_.subcord(0, size));
        cache_.remove_prefix(size);
        result->append(std::move(tmp));
        return size;
    }

    turbo::Result<size_t> BufferedReader::read(size_t size, turbo::Nonnull<std::string *> result) {
        if (!reach_end_) {
            auto rs = fill_buffer(size);
            if (!rs.ok()) {
                return rs.status();
            }
        }
        if (cache_.empty()) {
            return 0;
        }
        auto old_size = result->size();
        result->reserve(size + old_size);
        if (size >= cache_.size()) {
            turbo::append_cord_to_string(cache_, result);
            cache_.clear();
            return cache_.size();
        }
        auto need_copy = size;
        for (std::string_view sv: cache_.chunks()) {
            if (need_copy <= 0) {
                break;
            }
            auto copy_size = std::min(need_copy, sv.size());
            result->append(sv.data(), copy_size);
            need_copy -= copy_size;
        }
        cache_.remove_prefix(size);
        return size;
    }

    turbo::Result<size_t> BufferedReader::read(size_t size, turbo::Nonnull<uint8_t*> result) {
        if (!reach_end_) {
            auto rs = fill_buffer(size);
            if (!rs.ok()) {
                return rs.status();
            }
        }
        if (cache_.empty()) {
            return 0;
        }

        auto need_copy = std::min(size, cache_.size());
        uint8_t *dst = result;
        for (std::string_view sv: cache_.chunks()) {
            if (need_copy <= 0) {
                break;
            }
            auto copy_size = std::min(need_copy, sv.size());
            std::memcpy(dst, sv.data(), copy_size);
            dst += copy_size;
            need_copy -= copy_size;
        }
        cache_.remove_prefix(size);
        return size;
    }
    void BufferedReader::set_cache_size(size_t size) {
        if(size < kDefaultCacheSize) {
            cache_size_ = kDefaultCacheSize;
        } else {
            cache_size_ = size;
        }
    }

    turbo::Result<size_t> BufferedReader::fill_buffer(size_t size) {
        if (reach_end_) {
            return 0;
        }
        if (cache_.size() >= size) {
            return cache_.size();
        }
        auto need_size = std::max(size - cache_.size(), cache_size_ - cache_.size());
        auto rs = reader_->read(&cache_, need_size);
        if (!rs.ok()) {
            return rs.status();
        }
        if (rs.value() < need_size) {
            reach_end_ = true;
        }
        return rs.value();
    }
}  // namespace alkaid