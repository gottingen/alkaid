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

#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>
#include <set>

namespace alkaid {
    namespace internals {
        class IBasicCSVParser;
    }

    class CSVReader;

    /** Determines how to handle rows that are shorter or longer than the majority */
    enum class VariableColumnPolicy {
        THROW = -1,
        IGNORE_ROW = 0,
        KEEP = 1
    };

    /** Stores the inferred format of a CSV file. */
    struct CSVGuessResult {
        char delim;
        int header_row;
    };

    /** Stores information about how to parse a CSV file.
     *  Can be used to construct a csv::CSVReader.
     */
    class CSVFormat {
    public:
        /** Settings for parsing a RFC 4180 CSV file */
        CSVFormat() = default;

        /** Sets the delimiter of the CSV file
         *
         *  @throws `std::runtime_error` thrown if trim, quote, or possible delimiting characters overlap
         */
        CSVFormat &delimiter(char delim);

        /** Sets a list of potential delimiters
         *
         *  @throws `std::runtime_error` thrown if trim, quote, or possible delimiting characters overlap
         *  @param[in] delim An array of possible delimiters to try parsing the CSV with
         */
        CSVFormat &delimiter(const std::vector<char> &delim);

        /** Sets the whitespace characters to be trimmed
         *
         *  @throws `std::runtime_error` thrown if trim, quote, or possible delimiting characters overlap
         *  @param[in] ws An array of whitespace characters that should be trimmed
         */
        CSVFormat &trim(const std::vector<char> &ws);

        /** Sets the quote character
         *
         *  @throws `std::runtime_error` thrown if trim, quote, or possible delimiting characters overlap
         */
        CSVFormat &quote(char quote);

        /** Sets the column names.
         *
         *  @note Unsets any values set by header_row()
         */
        CSVFormat &column_names(const std::vector<std::string> &names);

        /** Sets the header row
         *
         *  @note Unsets any values set by column_names()
         */
        CSVFormat &header_row(int row);

        /** Tells the parser that this CSV has no header row
         *
         *  @note Equivalent to `header_row(-1)`
         *
         */
        CSVFormat &no_header() {
            this->header_row(-1);
            return *this;
        }

        /** Turn quoting on or off */
        CSVFormat &quote(bool use_quote) {
            this->no_quote = !use_quote;
            return *this;
        }

        /** Tells the parser how to handle columns of a different length than the others */
        constexpr CSVFormat &variable_columns(VariableColumnPolicy policy = VariableColumnPolicy::IGNORE_ROW) {
            this->variable_column_policy = policy;
            return *this;
        }

        /** Tells the parser how to handle columns of a different length than the others */
        constexpr CSVFormat &variable_columns(bool policy) {
            this->variable_column_policy = (VariableColumnPolicy) policy;
            return *this;
        }

#ifndef DOXYGEN_SHOULD_SKIP_THIS

        char get_delim() const {
            // This error should never be received by end users.
            if (this->possible_delimiters.size() > 1) {
                throw std::runtime_error("There is more than one possible delimiter.");
            }

            return this->possible_delimiters.at(0);
        }

        constexpr bool is_quoting_enabled() const { return !this->no_quote; }

        constexpr char get_quote_char() const { return this->quote_char; }

        constexpr int get_header() const { return this->header; }

        std::vector<char> get_possible_delims() const { return this->possible_delimiters; }

        std::vector<char> get_trim_chars() const { return this->trim_chars; }

        constexpr VariableColumnPolicy get_variable_column_policy() const { return this->variable_column_policy; }

#endif

        /** CSVFormat for guessing the delimiter */
        inline static CSVFormat guess_csv() {
            CSVFormat format;
            format.delimiter({',', '|', '\t', ';', '^'})
                    .quote('"')
                    .header_row(0);

            return format;
        }

        bool guess_delim() {
            return this->possible_delimiters.size() > 1;
        }

        friend CSVReader;
        friend internals::IBasicCSVParser;

    private:
        /**< Throws an error if delimiters and trim characters overlap */
        void assert_no_char_overlap();

        /**< Set of possible delimiters */
        std::vector<char> possible_delimiters = {','};

        /**< Set of whitespace characters to trim */
        std::vector<char> trim_chars = {};

        /**< Row number with columns (ignored if col_names is non-empty) */
        int header = 0;

        /**< Whether or not to use quoting */
        bool no_quote = false;

        /**< Quote character */
        char quote_char = '"';

        /**< Should be left empty unless file doesn't include header */
        std::vector<std::string> col_names = {};

        /**< Allow variable length columns? */
        VariableColumnPolicy variable_column_policy = VariableColumnPolicy::IGNORE_ROW;
    };

    /// inlines

    inline CSVFormat &CSVFormat::delimiter(char delim) {
        this->possible_delimiters = {delim};
        this->assert_no_char_overlap();
        return *this;
    }

    inline CSVFormat &CSVFormat::delimiter(const std::vector<char> &delim) {
        this->possible_delimiters = delim;
        this->assert_no_char_overlap();
        return *this;
    }

    inline CSVFormat &CSVFormat::quote(char quote) {
        this->no_quote = false;
        this->quote_char = quote;
        this->assert_no_char_overlap();
        return *this;
    }

    inline CSVFormat &CSVFormat::trim(const std::vector<char> &chars) {
        this->trim_chars = chars;
        this->assert_no_char_overlap();
        return *this;
    }

    inline CSVFormat &CSVFormat::column_names(const std::vector<std::string> &names) {
        this->col_names = names;
        this->header = -1;
        return *this;
    }

    inline CSVFormat &CSVFormat::header_row(int row) {
        if (row < 0) this->variable_column_policy = VariableColumnPolicy::KEEP;

        this->header = row;
        this->col_names = {};
        return *this;
    }

    inline void CSVFormat::assert_no_char_overlap() {
        auto delims = std::set<char>(
                this->possible_delimiters.begin(), this->possible_delimiters.end()),
                trims = std::set<char>(
                this->trim_chars.begin(), this->trim_chars.end());

        // Stores intersection of possible delimiters and trim characters
        std::vector<char> intersection = {};

        // Find which characters overlap, if any
        std::set_intersection(
                delims.begin(), delims.end(),
                trims.begin(), trims.end(),
                std::back_inserter(intersection));

        // Make sure quote character is not contained in possible delimiters
        // or whitespace characters
        if (delims.find(this->quote_char) != delims.end() ||
            trims.find(this->quote_char) != trims.end()) {
            intersection.push_back(this->quote_char);
        }

        if (!intersection.empty()) {
            std::string err_msg = "There should be no overlap between the quote character, "
                                  "the set of possible delimiters "
                                  "and the set of whitespace characters. Offending characters: ";

            // Create a pretty error message with the list of overlapping
            // characters
            for (size_t i = 0; i < intersection.size(); i++) {
                err_msg += "'";
                err_msg += intersection[i];
                err_msg += "'";

                if (i + 1 < intersection.size())
                    err_msg += ", ";
            }

            throw std::runtime_error(err_msg + '.');
        }
    }
}  // namespace alkaid

