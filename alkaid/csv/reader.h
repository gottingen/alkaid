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
#include <deque>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <thread>
#include <sstream>
#include <string>
#include <vector>

#include <alkaid/files/local/mmap.h>
#include <alkaid/csv/basic_parser.h>
#include <alkaid/csv/data_type.h>
#include <alkaid/csv/format.h>

/** The all encompassing namespace */
namespace alkaid {
    /** Stuff that is generally not of interest to end-users */
    namespace internals {
        std::string format_row(const std::vector<std::string> &row, std::string_view delim = ", ");

        std::vector<std::string> _get_col_names(std::string_view head, const CSVFormat format = CSVFormat::guess_csv());

        struct GuessScore {
            double score;
            size_t header;
        };

        inline GuessScore calculate_score(std::string_view head, CSVFormat format);

        CSVGuessResult
        _guess_format(std::string_view head, const std::vector<char> &delims = {',', '|', '\t', ';', '^', '~'});
    }

    std::vector<std::string> get_col_names(
            std::string_view filename,
            const CSVFormat format = CSVFormat::guess_csv());

    /** Guess the delimiter used by a delimiter-separated values file */
    CSVGuessResult guess_format(std::string_view filename,
                                const std::vector<char> &delims = {',', '|', '\t', ';', '^', '~'});

    /** @class CSVReader
     *  @brief Main class for parsing CSVs from files and in-memory sources
     *
     *  All rows are compared to the column names for length consistency
     *  - By default, rows that are too short or too long are dropped
     *  - Custom behavior can be defined by overriding bad_row_handler in a subclass
     */
    class CSVReader {
    public:
        /**
         * An input iterator capable of handling large files.
         * @note Created by CSVReader::begin() and CSVReader::end().
         *
         * @par Iterating over a file
         * @snippet tests/test_csv_iterator.cpp CSVReader Iterator 1
         *
         * @par Using with `<algorithm>` library
         * @snippet tests/test_csv_iterator.cpp CSVReader Iterator 2
         */
        class iterator {
        public:
#ifndef DOXYGEN_SHOULD_SKIP_THIS
            using value_type = CSVRow;
            using difference_type = std::ptrdiff_t;
            using pointer = CSVRow *;
            using reference = CSVRow &;
            using iterator_category = std::input_iterator_tag;
#endif

            iterator() = default;

            iterator(CSVReader *reader) : daddy(reader) {};

            iterator(CSVReader *, CSVRow &&);

            /** Access the CSVRow held by the iterator */
            constexpr reference operator*() { return this->row; }

            /** Return a pointer to the CSVRow the iterator has stopped at */
            constexpr pointer operator->() { return &(this->row); }

            iterator &operator++();   /**< Pre-increment iterator */
            iterator operator++(int); /**< Post-increment iterator */

            /** Returns true if iterators were constructed from the same CSVReader
             *  and point to the same row
             */
            constexpr bool operator==(const iterator &other) const noexcept {
                return (this->daddy == other.daddy) && (this->i == other.i);
            }

            constexpr bool operator!=(const iterator &other) const noexcept { return !operator==(other); }

        private:
            CSVReader *daddy = nullptr;  // Pointer to parent
            CSVRow row;                   // Current row
            size_t i = 0;               // Index of current row
        };

        /** @name Constructors
         *  Constructors for iterating over large files and parsing in-memory sources.
         */
        ///@{
        CSVReader(std::string_view filename, CSVFormat format = CSVFormat::guess_csv());

        /** Allows parsing stream sources such as `std::stringstream` or `std::ifstream`
         *
         *  @tparam TStream An input stream deriving from `std::istream`
         *  @note   Currently this constructor requires special CSV dialects to be manually
         *          specified.
         */
        template<typename TStream,
                std::enable_if_t<std::is_base_of<std::istream, TStream>::value, int> = 0>
        CSVReader(TStream &source, CSVFormat format = CSVFormat()) : _format(format) {
            using Parser = internals::StreamParser<TStream>;

            if (!format.col_names.empty())
                this->set_col_names(format.col_names);

            this->parser = std::unique_ptr<Parser>(
                    new Parser(source, format, col_names)); // For C++11
            this->initial_read();
        }
        ///@}

        CSVReader(const CSVReader &) = delete; // No copy constructor
        CSVReader(CSVReader &&) = default;     // Move constructor
        CSVReader &operator=(const CSVReader &) = delete; // No copy assignment
        CSVReader &operator=(CSVReader &&other) = default;

        ~CSVReader() {
            if (this->read_csv_worker.joinable()) {
                this->read_csv_worker.join();
            }
        }

        /** @name Retrieving CSV Rows */
        ///@{
        bool read_row(CSVRow &row);

        iterator begin();

        iterator end() const noexcept;

        /** Returns true if we have reached end of file */
        bool eof() const noexcept { return this->parser->eof(); };
        ///@}

        /** @name CSV Metadata */
        ///@{
        CSVFormat get_format() const;

        std::vector<std::string> get_col_names() const;

        int index_of(std::string_view col_name) const;
        ///@}

        /** @name CSV Metadata: Attributes */
        ///@{
        /** Whether or not the file or stream contains valid CSV rows,
         *  not including the header.
         *
         *  @note Gives an accurate answer regardless of when it is called.
         *
         */
        constexpr bool empty() const noexcept { return this->n_rows() == 0; }

        /** Retrieves the number of rows that have been read so far */
        constexpr size_t n_rows() const noexcept { return this->_n_rows; }

        /** Whether or not CSV was prefixed with a UTF-8 bom */
        bool utf8_bom() const noexcept { return this->parser->utf8_bom(); }
        ///@}

    protected:
        /**
         * \defgroup csv_internal CSV Parser Internals
         * @brief Internals of CSVReader. Only maintainers and those looking to
         *        extend the parser should read this.
         * @{
         */

        /** Sets this reader's column names and associated data */
        void set_col_names(const std::vector<std::string> &);

        /** @name CSV Settings **/
        ///@{
        CSVFormat _format;
        ///@}

        /** @name Parser State */
        ///@{
        /** Pointer to a object containing column information */
        internals::ColNamesPtr col_names = std::make_shared<internals::ColNames>();

        /** Helper class which actually does the parsing */
        std::unique_ptr<internals::IBasicCSVParser> parser = nullptr;

        /** Queue of parsed CSV rows */
        std::unique_ptr<RowCollection> records{new RowCollection(100)};

        size_t n_cols = 0;  /**< The number of columns in this CSV */
        size_t _n_rows = 0; /**< How many rows (minus header) have been read so far */

        /** @name Multi-Threaded File Reading Functions */
        ///@{
        bool read_csv(size_t bytes = internals::ITERATION_CHUNK_SIZE);
        ///@}

        /**@}*/

    private:
        /** Whether or not rows before header were trimmed */
        bool header_trimmed = false;

        /** @name Multi-Threaded File Reading: Flags and State */
        ///@{
        std::thread read_csv_worker; /**< Worker thread for read_csv() */
        ///@}

        /** Read initial chunk to get metadata */
        void initial_read() {
            this->read_csv_worker = std::thread(&CSVReader::read_csv, this, internals::ITERATION_CHUNK_SIZE);
            this->read_csv_worker.join();
        }

        void trim_header();
    };
    namespace internals {
        inline std::string format_row(const std::vector<std::string> &row, std::string_view delim) {
            /** Print a CSV row */
            std::stringstream ret;
            for (size_t i = 0; i < row.size(); i++) {
                ret << row[i];
                if (i + 1 < row.size()) ret << delim;
                else ret << '\n';
            }
            ret.flush();

            return ret.str();
        }

        /** Return a CSV's column names
         *
         *  @param[in] filename  Path to CSV file
         *  @param[in] format    Format of the CSV file
         *
         */
        inline std::vector<std::string> _get_col_names(std::string_view head, CSVFormat format) {
            // Parse the CSV
            auto trim_chars = format.get_trim_chars();
            std::stringstream source(head.data());
            RowCollection rows;

            StreamParser<std::stringstream> parser(source, format);
            parser.set_output(rows);
            parser.next();

            return CSVRow(std::move(rows[format.get_header()]));
        }

        inline GuessScore calculate_score(std::string_view head, CSVFormat format) {
            // Frequency counter of row length
            std::unordered_map<size_t, size_t> row_tally = {{0, 0}};

            // Map row lengths to row num where they first occurred
            std::unordered_map<size_t, size_t> row_when = {{0, 0}};

            // Parse the CSV
            std::stringstream source(head.data());
            RowCollection rows;

            StreamParser<std::stringstream> parser(source, format);
            parser.set_output(rows);
            parser.next();

            for (size_t i = 0; i < rows.size(); i++) {
                auto &row = rows[i];

                // Ignore zero-length rows
                if (row.size() > 0) {
                    if (row_tally.find(row.size()) != row_tally.end()) {
                        row_tally[row.size()]++;
                    } else {
                        row_tally[row.size()] = 1;
                        row_when[row.size()] = i;
                    }
                }
            }

            double final_score = 0;
            size_t header_row = 0;

            // Final score is equal to the largest
            // row size times rows of that size
            for (auto &pair: row_tally) {
                auto row_size = pair.first;
                auto row_count = pair.second;
                double score = (double) (row_size * row_count);
                if (score > final_score) {
                    final_score = score;
                    header_row = row_when[row_size];
                }
            }

            return {
                    final_score,
                    header_row
            };
        }

            /** Guess the delimiter used by a delimiter-separated values file */
            /** For each delimiter, find out which row length was most common.
             *  The delimiter with the longest mode row length wins.
             *  Then, the line number of the header row is the first row with
             *  the mode row length.
             */
        inline CSVGuessResult _guess_format(std::string_view head, const std::vector<char> &delims) {
            CSVFormat format;
            size_t max_score = 0,
                    header = 0;
            char current_delim = delims[0];

            for (char cand_delim: delims) {
                auto result = calculate_score(head, format.delimiter(cand_delim));

                if ((size_t) result.score > max_score) {
                    max_score = (size_t) result.score;
                    current_delim = cand_delim;
                    header = result.header;
                }
            }

            return {current_delim, (int) header};
        }
    }

    /** Return a CSV's column names
     *
     *  @param[in] filename  Path to CSV file
     *  @param[in] format    Format of the CSV file
     *
     */
    inline std::vector<std::string> get_col_names(std::string_view filename, CSVFormat format) {
        auto head = internals::get_csv_head(filename);

        /** Guess delimiter and header row */
        if (format.guess_delim()) {
            auto guess_result = guess_format(filename, format.get_possible_delims());
            format.delimiter(guess_result.delim).header_row(guess_result.header_row);
        }

        return internals::_get_col_names(head, format);
    }

        /** Guess the delimiter used by a delimiter-separated values file */
    inline CSVGuessResult guess_format(std::string_view filename, const std::vector<char> &delims) {
        auto head = internals::get_csv_head(filename);
        return internals::_guess_format(head, delims);
    }

    /** Reads an arbitrarily large CSV file using memory-mapped IO.
     *
     *  **Details:** Reads the first block of a CSV file synchronously to get information
     *               such as column names and delimiting character.
     *
     *  @param[in] filename  Path to CSV file
     *  @param[in] format    Format of the CSV file
     *
     *  \snippet tests/test_read_csv.cpp CSVField Example
     *
     */
    inline CSVReader::CSVReader(std::string_view filename, CSVFormat format) : _format(format) {
        auto head = internals::get_csv_head(filename);
        using Parser = internals::MmapParser;

        /** Guess delimiter and header row */
        if (format.guess_delim()) {
            auto guess_result = internals::_guess_format(head, format.possible_delimiters);
            format.delimiter(guess_result.delim);
            format.header = guess_result.header_row;
            this->_format = format;
        }

        if (!format.col_names.empty())
            this->set_col_names(format.col_names);

        this->parser = std::unique_ptr<Parser>(new Parser(filename, format, this->col_names)); // For C++11
        this->initial_read();
    }

    /** Return the format of the original raw CSV */
    inline CSVFormat CSVReader::get_format() const {
        CSVFormat new_format = this->_format;

        // Since users are normally not allowed to set
        // column names and header row simulatenously,
        // we will set the backing variables directly here
        new_format.col_names = this->col_names->get_col_names();
        new_format.header = this->_format.header;

        return new_format;
    }

    /** Return the CSV's column names as a vector of strings. */
    inline std::vector<std::string> CSVReader::get_col_names() const {
        if (this->col_names) {
            return this->col_names->get_col_names();
        }

        return std::vector<std::string>();
    }

    /** Return the index of the column name if found or
     *         csv::CSV_NOT_FOUND otherwise.
     */
    inline int CSVReader::index_of(std::string_view col_name) const {
        auto _col_names = this->get_col_names();
        for (size_t i = 0; i < _col_names.size(); i++)
            if (_col_names[i] == col_name) return (int) i;

        return CSV_NOT_FOUND;
    }

    inline void CSVReader::trim_header() {
        if (!this->header_trimmed) {
            for (int i = 0; i <= this->_format.header && !this->records->empty(); i++) {
                if (i == this->_format.header && this->col_names->empty()) {
                    this->set_col_names(this->records->pop_front());
                } else {
                    this->records->pop_front();
                }
            }

            this->header_trimmed = true;
        }
    }

    /**
     *  @param[in] names Column names
     */
    inline void CSVReader::set_col_names(const std::vector<std::string> &names) {
        this->col_names->set_col_names(names);
        this->n_cols = names.size();
    }

    /**
     * Read a chunk of CSV data.
     *
     * @note This method is meant to be run on its own thread. Only one `read_csv()` thread
     *       should be active at a time.
     *
     * @param[in] bytes Number of bytes to read.
     *
     * @see CSVReader::read_csv_worker
     * @see CSVReader::read_row()
     */
    inline bool CSVReader::read_csv(size_t bytes) {
        // Tell read_row() to listen for CSV rows
        this->records->notify_all();

        this->parser->set_output(*this->records);
        this->parser->next(bytes);

        if (!this->header_trimmed) {
            this->trim_header();
        }

        // Tell read_row() to stop waiting
        this->records->kill_all();

        return true;
    }

    /**
     * Retrieve rows as CSVRow objects, returning true if more rows are available.
     *
     * @par Performance Notes
     *  - Reads chunks of data that are csv::internals::ITERATION_CHUNK_SIZE bytes large at a time
     *  - For performance details, read the documentation for CSVRow and CSVField.
     *
     * @param[out] row The variable where the parsed row will be stored
     * @see CSVRow, CSVField
     *
     * **Example:**
     * \snippet tests/test_read_csv.cpp CSVField Example
     *
     */
    inline bool CSVReader::read_row(CSVRow &row) {
        while (true) {
            if (this->records->empty()) {
                if (this->records->is_waitable())
                    // Reading thread is currently active => wait for it to populate records
                    this->records->wait();
                else if (this->parser->eof())
                    // End of file and no more records
                    return false;
                else {
                    // Reading thread is not active => start another one
                    if (this->read_csv_worker.joinable())
                        this->read_csv_worker.join();

                    this->read_csv_worker = std::thread(&CSVReader::read_csv, this, internals::ITERATION_CHUNK_SIZE);
                }
            } else if (this->records->front().size() != this->n_cols &&
                       this->_format.variable_column_policy != VariableColumnPolicy::KEEP) {
                auto errored_row = this->records->pop_front();

                if (this->_format.variable_column_policy == VariableColumnPolicy::THROW) {
                    if (errored_row.size() < this->n_cols)
                        throw std::runtime_error("Line too short " + internals::format_row(errored_row));

                    throw std::runtime_error("Line too long " + internals::format_row(errored_row));
                }
            } else {
                row = this->records->pop_front();
                this->_n_rows++;
                return true;
            }
        }

        return false;
    }
    inline CSVReader::iterator CSVReader::begin() {
        if (this->records->empty()) {
            this->read_csv_worker = std::thread(&CSVReader::read_csv, this, internals::ITERATION_CHUNK_SIZE);
            this->read_csv_worker.join();

            // Still empty => return end iterator
            if (this->records->empty()) return this->end();
        }

        CSVReader::iterator ret(this, this->records->pop_front());
        return ret;
    }

    /** A placeholder for the imaginary past the end row in a CSV.
     *  Attempting to deference this will lead to bad things.
     */
    inline  CSVReader::iterator CSVReader::end() const noexcept {
        return CSVReader::iterator();
    }

    /////////////////////////
    // CSVReader::iterator //
    /////////////////////////

    inline CSVReader::iterator::iterator(CSVReader* _daddy, CSVRow&& _row) :
            daddy(_daddy) {
        row = std::move(_row);
    }

    /** Advance the iterator by one row. If this CSVReader has an
     *  associated file, then the iterator will lazily pull more data from
     *  that file until the end of file is reached.
     *
     *  @note This iterator does **not** block the thread responsible for parsing CSV.
     *
     */
    inline CSVReader::iterator& CSVReader::iterator::operator++() {
        if (!daddy->read_row(this->row)) {
            this->daddy = nullptr; // this == end()
        }

        return *this;
    }

    /** Post-increment iterator */
    inline CSVReader::iterator CSVReader::iterator::operator++(int) {
        auto temp = *this;
        if (!daddy->read_row(this->row)) {
            this->daddy = nullptr; // this == end()
        }

        return temp;
    }
}  // alkaid

