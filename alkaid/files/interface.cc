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

namespace alkaid {

    turbo::Result<size_t> SequentialFileReader::read(void *buff, size_t len) noexcept{
        return read_impl(buff, len);
    }

    turbo::Result<size_t> SequentialFileReader::read(std::string *result, size_t length) noexcept {
        auto len = length;
        if (len == kInfiniteFileSize) {
            auto status = size();
            if (!status.ok()) {
                return status;
            }
            len = status.value();
        }
        auto original_size = result->size();
        result->resize(original_size + len);
        auto status = read_impl(result->data() + original_size, len);
        if (!status.ok()) {
            result->resize(original_size);
            return status;
        }
        result->resize(original_size + status.value());
        return status;
    }

    turbo::Result<size_t> SequentialFileReader::read(turbo::Cord *buffer, size_t length) noexcept {
        auto len = length;
        if (len == kInfiniteFileSize) {
            auto status = size();
            if (!status.ok()) {
                return status;
            }
            len = status.value();
        }
        size_t total_read = 0;
        turbo::CordBuffer apbuff = buffer->get_append_buffer(len);
        turbo::span<char> first = apbuff.available_up_to(len);
        RESULT_ASSIGN_OR_RETURN(total_read, read(first.data(), first.size()));
        apbuff.increase_length_by(total_read);
        buffer->append(std::move(apbuff));
        if (total_read < first.size()) {
            return total_read;
        }
        while (total_read < len) {
            turbo::CordBuffer data = turbo::CordBuffer::create_with_default_limit(len - total_read);
            turbo::span<char> data_span = data.available_up_to(len - total_read);
            RESULT_ASSIGN_OR_RETURN(size_t read_size, read(data_span.data(), data_span.size()));
            data.increase_length_by(read_size);
            buffer->append(std::move(data));
            if (read_size < data_span.size()) {
                break;
            }
            total_read += read_size;
        }
        return total_read;
    }

    turbo::Result<size_t> RandomAccessFileReader::read_at(off_t offset, void *buff, size_t len) {
        return read_at_impl(offset, buff, len);
    }

    turbo::Result<size_t> RandomAccessFileReader::read_at(off_t offset, std::string *result, size_t length) {
        auto len = length;
        if (len == kInfiniteFileSize) {
            auto status = size();
            if (!status.ok()) {
                return status;
            }
            len = status.value();
        }
        auto original_size = result->size();
        result->resize(original_size + len);
        auto status = read_at_impl(offset, result->data() + original_size, len);
        if (!status.ok()) {
            result->resize(original_size);
            return status;
        }
        result->resize(original_size + status.value());
        return status;
    }

    turbo::Result<size_t> RandomAccessFileReader::read_at(off_t offset, turbo::Cord &buffer, size_t length) {
        auto len = length;
        if (len == kInfiniteFileSize) {
            auto status = size();
            if (!status.ok()) {
                return status;
            }
            len = status.value();
        }
        size_t total_read = 0;
        turbo::CordBuffer apbuff = buffer.get_append_buffer(len);
        turbo::span<char> first = apbuff.available_up_to(len);
        RESULT_ASSIGN_OR_RETURN(total_read, read_at_impl(offset, first.data(), first.size()));
        apbuff.increase_length_by(total_read);
        buffer.append(std::move(apbuff));
        if (total_read < first.size()) {
            return total_read;
        }
        while (total_read < len) {
            turbo::CordBuffer data = turbo::CordBuffer::create_with_default_limit(len - total_read);
            turbo::span<char> data_span = data.available_up_to(len - total_read);
            RESULT_ASSIGN_OR_RETURN(size_t read_size,
                                    read_at_impl(offset + total_read, data_span.data(), data_span.size()));
            data.increase_length_by(read_size);
            buffer.append(std::move(data));
            if (read_size < data_span.size()) {
                break;
            }
            total_read += read_size;
        }
        return total_read;
    }

    turbo::Status SequentialFileWriter::flush() {
        return turbo::OkStatus();
    }

    turbo::Status SequentialFileWriter::append(const void *buff, size_t length, bool trunc) {
        size_t original_size = 0;
        if (trunc) {
            auto rs = size();
            if (!rs.ok()) {
                return rs.status();
            }
            original_size = rs.value();
        }
        auto rs = append_impl(buff, length);
        if (!rs.ok()) {
            if (trunc) {
                auto trs = truncate(original_size);
                (void) trs;
            }
            return rs;
        }
        if(trunc) {
            auto trs = truncate(original_size + length);
            if(!trs.ok()) {
                return trs;
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status SequentialFileWriter::append(std::string_view buff, bool trunc) {
        size_t original_size;
        if (trunc) {
            auto rs = size();
            if (!rs.ok()) {
                return rs.status();
            }
            original_size = rs.value();
        }

        auto rs = append_impl(buff.data(), buff.size());
        if (!rs.ok()) {
            if (trunc) {
                auto trs = truncate(original_size);
                (void) trs;
            }
            return rs;
        }
        if(trunc) {
            auto trs = truncate(original_size + buff.size());
            if(!trs.ok()) {
                return trs;
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status SequentialFileWriter::append(const turbo::Cord &buffer, bool trunc) {
        size_t original_size{0};
        if (trunc) {
            auto rs = size();
            if (!rs.ok()) {
                return rs.status();
            }
            original_size = rs.value();
        }
        for (std::string_view view: buffer.chunks()) {
            auto rs = append_impl(view.data(), view.size());
            if (!rs.ok()) {
                if(trunc) {
                    auto trs = truncate(original_size);
                    return rs;
                }
            }
        }
        if(trunc) {
            auto trs = truncate(original_size + buffer.size());
            if(!trs.ok()) {
                return trs;
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status RandomAccessFileWriter::write_at(off_t offset, const void *buff, size_t len) {
        return write_at_impl(offset, buff, len);
    }

    turbo::Status RandomAccessFileWriter::write_at(off_t offset, std::string_view buff) {
        return write_at(offset, buff.data(), buff.size());

    }

    turbo::Status RandomAccessFileWriter::write_at(off_t offset, const turbo::Cord &buffer) {
        for (std::string_view view: buffer.chunks()) {
            auto rs = write_at(offset, view.data(), view.size());
            if (!rs.ok()) {
                return rs;
            }
            offset += view.size();
        }
        return turbo::OkStatus();
    }

    turbo::Status RandomAccessFileWriter::flush() {
        return turbo::OkStatus();
    }
}  // namespace alkaid
