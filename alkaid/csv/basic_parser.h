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

#include <algorithm>
#include <array>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <vector>

#include <alkaid/files/local/mmap.h>
#include <alkaid/csv/col_names.h>
#include <alkaid/csv/format.h>
#include <alkaid/csv/row.h>

namespace alkaid {
    namespace internals {
        /** Create a vector v where each index i corresponds to the
         *  ASCII number for a character and, v[i + 128] labels it according to
         *  the CSVReader::ParseFlags enum
         */
        constexpr ParseFlagMap make_parse_flags(char delimiter) {
            std::array<ParseFlags, 256> ret = {};
            for (int i = -128; i < 128; i++) {
                const int arr_idx = i + 128;
                char ch = char(i);

                if (ch == delimiter)
                    ret[arr_idx] = ParseFlags::DELIMITER;
                else if (ch == '\r' || ch == '\n')
                    ret[arr_idx] = ParseFlags::NEWLINE;
                else
                    ret[arr_idx] = ParseFlags::NOT_SPECIAL;
            }

            return ret;
        }

        /** Create a vector v where each index i corresponds to the
         *  ASCII number for a character and, v[i + 128] labels it according to
         *  the CSVReader::ParseFlags enum
         */
        constexpr ParseFlagMap make_parse_flags(char delimiter, char quote_char) {
            std::array<ParseFlags, 256> ret = make_parse_flags(delimiter);
            ret[(size_t) quote_char + 128] = ParseFlags::QUOTE;
            return ret;
        }

        /** Create a vector v where each index i corresponds to the
         *  ASCII number for a character c and, v[i + 128] is true if
         *  c is a whitespace character
         */
        constexpr WhitespaceMap make_ws_flags(const char *ws_chars, size_t n_chars) {
            std::array<bool, 256> ret = {};
            for (int i = -128; i < 128; i++) {
                const int arr_idx = i + 128;
                char ch = char(i);
                ret[arr_idx] = false;

                for (size_t j = 0; j < n_chars; j++) {
                    if (ws_chars[j] == ch) {
                        ret[arr_idx] = true;
                    }
                }
            }

            return ret;
        }

        inline WhitespaceMap make_ws_flags(const std::vector<char> &flags) {
            return make_ws_flags(flags.data(), flags.size());
        }

        inline size_t get_file_size(std::string_view filename);

        inline std::string get_csv_head(std::string_view filename);

        /** Read the first 500KB of a CSV file */
        inline std::string get_csv_head(std::string_view filename, size_t file_size);

        /** A std::deque wrapper which allows multiple read and write threads to concurrently
         *  access it along with providing read threads the ability to wait for the deque
         *  to become populated
         */
        template<typename T>
        class ThreadSafeDeque {
        public:
            ThreadSafeDeque(size_t notify_size = 100) : _notify_size(notify_size) {};

            ThreadSafeDeque(const ThreadSafeDeque &other) {
                this->data = other.data;
                this->_notify_size = other._notify_size;
            }

            ThreadSafeDeque(const std::deque<T> &source) : ThreadSafeDeque() {
                this->data = source;
            }

            void clear() noexcept { this->data.clear(); }

            bool empty() const noexcept {
                return this->data.empty();
            }

            T &front() noexcept {
                return this->data.front();
            }

            T &operator[](size_t n) {
                return this->data[n];
            }

            void push_back(T &&item) {
                std::lock_guard<std::mutex> lock{this->_lock};
                this->data.push_back(std::move(item));

                if (this->size() >= _notify_size) {
                    this->_cond.notify_all();
                }
            }

            T pop_front() noexcept {
                std::lock_guard<std::mutex> lock{this->_lock};
                T item = std::move(data.front());
                data.pop_front();
                return item;
            }

            size_t size() const noexcept { return this->data.size(); }

            /** Returns true if a thread is actively pushing items to this deque */
            constexpr bool is_waitable() const noexcept { return this->_is_waitable; }

            /** Wait for an item to become available */
            void wait() {
                if (!is_waitable()) {
                    return;
                }

                std::unique_lock<std::mutex> lock{this->_lock};
                this->_cond.wait(lock, [this] { return this->size() >= _notify_size || !this->is_waitable(); });
                lock.unlock();
            }

            typename std::deque<T>::iterator begin() noexcept {
                return this->data.begin();
            }

            typename std::deque<T>::iterator end() noexcept {
                return this->data.end();
            }

            /** Tell listeners that this deque is actively being pushed to */
            void notify_all() {
                std::unique_lock<std::mutex> lock{this->_lock};
                this->_is_waitable = true;
                this->_cond.notify_all();
            }

            /** Tell all listeners to stop */
            void kill_all() {
                std::unique_lock<std::mutex> lock{this->_lock};
                this->_is_waitable = false;
                this->_cond.notify_all();
            }

        private:
            bool _is_waitable = false;
            size_t _notify_size;
            std::mutex _lock;
            std::condition_variable _cond;
            std::deque<T> data;
        };

        constexpr const int UNINITIALIZED_FIELD = -1;
    }

    /** Standard type for storing collection of rows */
    using RowCollection = internals::ThreadSafeDeque<CSVRow>;

    namespace internals {
        /** Abstract base class which provides CSV parsing logic.
         *
         *  Concrete implementations may customize this logic across
         *  different input sources, such as memory mapped files, stringstreams,
         *  etc...
         */
        class IBasicCSVParser {
        public:
            IBasicCSVParser() = default;

            IBasicCSVParser(const CSVFormat &, const ColNamesPtr &);

            IBasicCSVParser(const ParseFlagMap &parse_flags, const WhitespaceMap &ws_flags
            ) : _parse_flags(parse_flags), _ws_flags(ws_flags) {};

            virtual ~IBasicCSVParser() {}

            /** Whether or not we have reached the end of source */
            bool eof() { return this->_eof; }

            /** Parse the next block of data */
            virtual void next(size_t bytes) = 0;

            /** Indicate the last block of data has been parsed */
            void end_feed();

            constexpr ParseFlags parse_flag(const char ch) const noexcept {
                return _parse_flags.data()[ch + 128];
            }

            constexpr ParseFlags compound_parse_flag(const char ch) const noexcept {
                return quote_escape_flag(parse_flag(ch), this->quote_escape);
            }

            /** Whether or not this CSV has a UTF-8 byte order mark */
            constexpr bool utf8_bom() const { return this->_utf8_bom; }

            void set_output(RowCollection &rows) { this->_records = &rows; }

        protected:
            /** @name Current Parser State */
            ///@{
            CSVRow current_row;
            RawCSVDataPtr data_ptr = nullptr;
            ColNamesPtr _col_names = nullptr;
            CSVFieldList *fields = nullptr;
            int field_start = UNINITIALIZED_FIELD;
            size_t field_length = 0;

            /** An array where the (i + 128)th slot gives the ParseFlags for ASCII character i */
            ParseFlagMap _parse_flags;
            ///@}

            /** @name Current Stream/File State */
            ///@{
            bool _eof = false;

            /** The size of the incoming CSV */
            size_t source_size = 0;
            ///@}

            /** Whether or not source needs to be read in chunks */
            constexpr bool no_chunk() const { return this->source_size < ITERATION_CHUNK_SIZE; }

            /** Parse the current chunk of data *
             *
             *  @returns How many character were read that are part of complete rows
             */
            size_t parse();

            /** Create a new RawCSVDataPtr for a new chunk of data */
            void reset_data_ptr();

        private:
            /** An array where the (i + 128)th slot determines whether ASCII character i should
             *  be trimmed
             */
            WhitespaceMap _ws_flags;
            bool quote_escape = false;
            bool field_has_double_quote = false;

            /** Where we are in the current data block */
            size_t data_pos = 0;

            /** Whether or not an attempt to find Unicode BOM has been made */
            bool unicode_bom_scan = false;
            bool _utf8_bom = false;

            /** Where complete rows should be pushed to */
            RowCollection *_records = nullptr;

            constexpr bool ws_flag(const char ch) const noexcept {
                return _ws_flags.data()[ch + 128];
            }

            size_t &current_row_start() {
                return this->current_row.data_start;
            }

            void parse_field() noexcept;

            /** Finish parsing the current field */
            void push_field();

            /** Finish parsing the current row */
            void push_row();

            /** Handle possible Unicode byte order mark */
            void trim_utf8_bom();
        };

        /** A class for parsing CSV data from a `std::stringstream`
         *  or an `std::ifstream`
         */
        template<typename TStream>
        class StreamParser : public IBasicCSVParser {
            using RowCollection = ThreadSafeDeque<CSVRow>;

        public:
            StreamParser(TStream &source,
                         const CSVFormat &format,
                         const ColNamesPtr &col_names = nullptr
            ) : IBasicCSVParser(format, col_names), _source(std::move(source)) {};

            StreamParser(
                    TStream &source,
                    internals::ParseFlagMap parse_flags,
                    internals::WhitespaceMap ws_flags) :
                    IBasicCSVParser(parse_flags, ws_flags),
                    _source(std::move(source)) {};

            ~StreamParser() {}

            void next(size_t bytes = ITERATION_CHUNK_SIZE) override {
                if (this->eof()) return;

                this->reset_data_ptr();
                this->data_ptr->_data = std::make_shared<std::string>();

                if (source_size == 0) {
                    const auto start = _source.tellg();
                    _source.seekg(0, std::ios::end);
                    const auto end = _source.tellg();
                    _source.seekg(0, std::ios::beg);

                    source_size = end - start;
                }

                // Read data into buffer
                size_t length = std::min(source_size - stream_pos, bytes);
                std::unique_ptr<char[]> buff(new char[length]);
                _source.seekg(stream_pos, std::ios::beg);
                _source.read(buff.get(), length);
                stream_pos = _source.tellg();
                ((std::string *) (this->data_ptr->_data.get()))->assign(buff.get(), length);

                // Create string_view
                this->data_ptr->data = *((std::string *) this->data_ptr->_data.get());

                // Parse
                this->current_row = CSVRow(this->data_ptr);
                size_t remainder = this->parse();

                if (stream_pos == source_size || no_chunk()) {
                    this->_eof = true;
                    this->end_feed();
                } else {
                    this->stream_pos -= (length - remainder);
                }
            }

        private:
            TStream _source;
            size_t stream_pos = 0;
        };

        /** Parser for memory-mapped files
         *
         *  @par Implementation
         *  This class constructs moving windows over a file to avoid
         *  creating massive memory maps which may require more RAM
         *  than the user has available. It contains logic to automatically
         *  re-align each memory map to the beginning of a CSV row.
         *
         */
        class MmapParser : public IBasicCSVParser {
        public:
            MmapParser(std::string_view filename,
                       const CSVFormat &format,
                       const ColNamesPtr &col_names = nullptr
            ) : IBasicCSVParser(format, col_names) {
                this->_filename = filename.data();
                this->source_size = get_file_size(filename);
            };

            ~MmapParser() {}

            void next(size_t bytes) override;

        private:
            std::string _filename;
            size_t mmap_pos = 0;
        };
    }  // namespace internals
    namespace internals {
        inline size_t get_file_size(std::string_view filename) {
            std::ifstream infile(std::string(filename), std::ios::binary);
            const auto start = infile.tellg();
            infile.seekg(0, std::ios::end);
            const auto end = infile.tellg();

            return end - start;
        }

        inline std::string get_csv_head(std::string_view filename) {
            return get_csv_head(filename, get_file_size(filename));
        }

        inline std::string get_csv_head(std::string_view filename, size_t file_size) {
            const size_t bytes = 500000;

            std::error_code error;
            size_t length = std::min((size_t) file_size, bytes);
            auto mmap = make_mmap_source(std::string(filename), 0, length, error);

            if (error) {
                throw std::runtime_error("Cannot open file " + std::string(filename));
            }

            return std::string(mmap.begin(), mmap.end());
        }

#ifdef _MSC_VER
#pragma region IBasicCVParser
#endif

        inline IBasicCSVParser::IBasicCSVParser(
                const CSVFormat &format,
                const ColNamesPtr &col_names
        ) : _col_names(col_names) {
            if (format.no_quote) {
                _parse_flags = internals::make_parse_flags(format.get_delim());
            } else {
                _parse_flags = internals::make_parse_flags(format.get_delim(), format.quote_char);
            }

            _ws_flags = internals::make_ws_flags(
                    format.trim_chars.data(), format.trim_chars.size()
            );
        }

        inline void IBasicCSVParser::end_feed() {
            using internals::ParseFlags;

            bool empty_last_field = this->data_ptr
                                    && this->data_ptr->_data
                                    && !this->data_ptr->data.empty()
                                    && (parse_flag(this->data_ptr->data.back()) == ParseFlags::DELIMITER
                                        || parse_flag(this->data_ptr->data.back()) == ParseFlags::QUOTE);

            // Push field
            if (this->field_length > 0 || empty_last_field) {
                this->push_field();
            }

            // Push row
            if (this->current_row.size() > 0)
                this->push_row();
        }

        inline void IBasicCSVParser::parse_field() noexcept {
            using internals::ParseFlags;
            auto &in = this->data_ptr->data;

            // Trim off leading whitespace
            while (data_pos < in.size() && ws_flag(in[data_pos]))
                data_pos++;

            if (field_start == UNINITIALIZED_FIELD)
                field_start = (int) (data_pos - current_row_start());

            // Optimization: Since NOT_SPECIAL characters tend to occur in contiguous
            // sequences, use the loop below to avoid having to go through the outer
            // switch statement as much as possible
            while (data_pos < in.size() && compound_parse_flag(in[data_pos]) == ParseFlags::NOT_SPECIAL)
                data_pos++;

            field_length = data_pos - (field_start + current_row_start());

            // Trim off trailing whitespace, this->field_length constraint matters
            // when field is entirely whitespace
            for (size_t j = data_pos - 1; ws_flag(in[j]) && this->field_length > 0; j--)
                this->field_length--;
        }

        inline void IBasicCSVParser::push_field() {
            // Update
            if (field_has_double_quote) {
                fields->emplace_back(
                        field_start == UNINITIALIZED_FIELD ? 0 : (unsigned int) field_start,
                        field_length,
                        true
                );
                field_has_double_quote = false;

            } else {
                fields->emplace_back(
                        field_start == UNINITIALIZED_FIELD ? 0 : (unsigned int) field_start,
                        field_length
                );
            }

            current_row.row_length++;

            // Reset field state
            field_start = UNINITIALIZED_FIELD;
            field_length = 0;
        }

/** @return The number of characters parsed that belong to complete rows */
        inline size_t IBasicCSVParser::parse() {
            using internals::ParseFlags;

            this->quote_escape = false;
            this->data_pos = 0;
            this->current_row_start() = 0;
            this->trim_utf8_bom();

            auto &in = this->data_ptr->data;
            while (this->data_pos < in.size()) {
                switch (compound_parse_flag(in[this->data_pos])) {
                    case ParseFlags::DELIMITER:
                        this->push_field();
                        this->data_pos++;
                        break;

                    case ParseFlags::NEWLINE:
                        this->data_pos++;

                        // Catches CRLF (or LFLF)
                        if (this->data_pos < in.size() && parse_flag(in[this->data_pos]) == ParseFlags::NEWLINE)
                            this->data_pos++;

                        // End of record -> Write record
                        this->push_field();
                        this->push_row();

                        // Reset
                        this->current_row = CSVRow(data_ptr, this->data_pos, fields->size());
                        break;

                    case ParseFlags::NOT_SPECIAL:
                        this->parse_field();
                        break;

                    case ParseFlags::QUOTE_ESCAPE_QUOTE:
                        if (data_pos + 1 == in.size()) return this->current_row_start();
                        else if (data_pos + 1 < in.size()) {
                            auto next_ch = parse_flag(in[data_pos + 1]);
                            if (next_ch >= ParseFlags::DELIMITER) {
                                quote_escape = false;
                                data_pos++;
                                break;
                            } else if (next_ch == ParseFlags::QUOTE) {
                                // Case: Escaped quote
                                data_pos += 2;
                                this->field_length += 2;
                                this->field_has_double_quote = true;
                                break;
                            }
                        }

                        // Case: Unescaped single quote => not strictly valid but we'll keep it
                        this->field_length++;
                        data_pos++;

                        break;

                    default: // Quote (currently not quote escaped)
                        if (this->field_length == 0) {
                            quote_escape = true;
                            data_pos++;
                            if (field_start == UNINITIALIZED_FIELD && data_pos < in.size() && !ws_flag(in[data_pos]))
                                field_start = (int) (data_pos - current_row_start());
                            break;
                        }

                        // Case: Unescaped quote
                        this->field_length++;
                        data_pos++;

                        break;
                }
            }

            return this->current_row_start();
        }

        inline void IBasicCSVParser::push_row() {
            current_row.row_length = fields->size() - current_row.fields_start;
            this->_records->push_back(std::move(current_row));
        }

        inline void IBasicCSVParser::reset_data_ptr() {
            this->data_ptr = std::make_shared<RawCSVData>();
            this->data_ptr->parse_flags = this->_parse_flags;
            this->data_ptr->col_names = this->_col_names;
            this->fields = &(this->data_ptr->fields);
        }

        inline void IBasicCSVParser::trim_utf8_bom() {
            auto &data = this->data_ptr->data;

            if (!this->unicode_bom_scan && data.size() >= 3) {
                if (data[0] == '\xEF' && data[1] == '\xBB' && data[2] == '\xBF') {
                    this->data_pos += 3; // Remove BOM from input string
                    this->_utf8_bom = true;
                }

                this->unicode_bom_scan = true;
            }
        }

#ifdef _MSC_VER
#pragma endregion
#endif

#ifdef _MSC_VER
#pragma region Specializations
#endif

        inline void MmapParser::next(size_t bytes = ITERATION_CHUNK_SIZE) {
            // Reset parser state
            this->field_start = UNINITIALIZED_FIELD;
            this->field_length = 0;
            this->reset_data_ptr();

            // Create memory map
            size_t length = std::min(this->source_size - this->mmap_pos, bytes);
            std::error_code error;
            this->data_ptr->_data = std::make_shared<basic_mmap_source<char>>(
                    make_mmap_source(this->_filename, this->mmap_pos, length, error));
            this->mmap_pos += length;
            if (error) throw error;

            auto mmap_ptr = (basic_mmap_source<char> *) (this->data_ptr->_data.get());

            // Create string view
            this->data_ptr->data = std::string_view(mmap_ptr->data(), mmap_ptr->length());

            // Parse
            this->current_row = CSVRow(this->data_ptr);
            size_t remainder = this->parse();

            if (this->mmap_pos == this->source_size || no_chunk()) {
                this->_eof = true;
                this->end_feed();
            }

            this->mmap_pos -= (length - remainder);
        }

#ifdef _MSC_VER
#pragma endregion
#endif
    }
}  // namespace alkaid

