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

#include <alkaid/files/buffered_writer.h>
#include <turbo/log/logging.h>

namespace alkaid {

    BufferedWriter::BufferedWriter(const std::shared_ptr<SequentialFileWriter> &writer, size_t cache_size)
            : cache_size_(cache_size), writer_(writer) {
        CHECK(writer_)<<"writer is nullptr";
    }
    BufferedWriter::~BufferedWriter() {
        CHECK(finalized_) << "BufferedWriter is destructed without calling finalize()";
    }

    turbo::Status BufferedWriter::write(const turbo::Cord &cord) {
        CHECK(!finalized_)<<"BufferedWriter is finalized, you can not write any more data after finalize()";
        cache_.append(cord);
        if (cache_.size() >= cache_size_) {
            return flush_impl();
        }
        return turbo::OkStatus();
    }

    turbo::Status BufferedWriter::write(const std::string &str) {
        CHECK(!finalized_)<<"BufferedWriter is finalized, you can not write any more data after finalize()";
        cache_.append(str);
        if (cache_.size() >= cache_size_) {
            return flush_impl();
        }
        return turbo::OkStatus();
    }

    turbo::Status BufferedWriter::write(const turbo::Nonnull<const uint8_t *> data, size_t size) {
        CHECK(!finalized_)<<"BufferedWriter is finalized, you can not write any more data after finalize()";
        cache_.append(std::string_view{reinterpret_cast<const char*>(data), size});
        if (cache_.size() >= cache_size_) {
            return flush_impl();
        }
        return turbo::OkStatus();
    }

    turbo::Status BufferedWriter::flush_impl() {
        CHECK(!finalized_)<<"BufferedWriter is finalized, you can not write any more data after finalize()";
        auto rs = writer_->append(cache_, true);
        if (!rs.ok()) {
            return rs;
        }
        cache_.clear();
        return turbo::OkStatus();
    }

    turbo::Status BufferedWriter::flush() {
        if(cache_.empty()) {
            return turbo::OkStatus();
        }
        return flush_impl();
    }

    turbo::Status BufferedWriter::finalize() {
        if(finalized_) {
            return turbo::OkStatus();
        }
        auto rs = flush();
        if(!rs.ok()) {
            return rs;
        }
        rs = writer_->flush();
        if(!rs.ok()) {
            return rs;
        }
        finalized_ = true;
        return turbo::OkStatus();
    }

}  // namespace alkaid
