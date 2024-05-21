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

#include <alkaid/csv/format.h>
#include <alkaid/csv/reader.h>
#include <alkaid/csv/data_type.h>

#include <string>
#include <type_traits>
#include <unordered_map>

namespace alkaid {
    /** Returned by get_file_info() */
    struct CSVFileInfo {
        std::string filename;               /**< Filename */
        std::vector<std::string> col_names; /**< CSV column names */
        char delim;                         /**< Delimiting character */
        size_t n_rows;                      /**< Number of rows in a file */
        size_t n_cols;                      /**< Number of columns in a CSV */
    };

    /** @name Shorthand Parsing Functions
     *  @brief Convienience functions for parsing small strings
     */
    ///@{
    CSVReader operator ""_csv(const char *, size_t);

    CSVReader operator ""_csv_no_header(const char *, size_t);

    CSVReader parse(std::string_view in, CSVFormat format = CSVFormat());

    CSVReader parse_no_header(std::string_view in);
    ///@}

    /** @name Utility Functions */
    ///@{
    std::unordered_map<std::string, DataType> csv_data_types(const std::string &);

    CSVFileInfo get_file_info(const std::string &filename);

    int get_col_pos(std::string_view filename, std::string_view col_name,
                    const CSVFormat &format = CSVFormat::guess_csv());
    ///@}
    /** Shorthand function for parsing an in-memory CSV string
    *
    *  @return A collection of CSVRow objects
    *
    *  @par Example
    *  @snippet tests/test_read_csv.cpp Parse Example
    */
    inline CSVReader parse(std::string_view in, CSVFormat format) {
        std::stringstream stream(in.data());
        return CSVReader(stream, format);
    }

    /** Parses a CSV string with no headers
     *
     *  @return A collection of CSVRow objects
     */
    inline CSVReader parse_no_header(std::string_view in) {
        CSVFormat format;
        format.header_row(-1);

        return parse(in, format);
    }

    /** Parse a RFC 4180 CSV string, returning a collection
     *  of CSVRow objects
     *
     *  @par Example
     *  @snippet tests/test_read_csv.cpp Escaped Comma
     *
     */
    inline CSVReader operator ""_csv(const char *in, size_t n) {
        return parse(std::string_view(in, n));
    }

    /** A shorthand for csv::parse_no_header() */
    inline CSVReader operator ""_csv_no_header(const char *in, size_t n) {
        return parse_no_header(std::string_view(in, n));
    }

    /**
     *  Find the position of a column in a CSV file or CSV_NOT_FOUND otherwise
     *
     *  @param[in] filename  Path to CSV file
     *  @param[in] col_name  Column whose position we should resolve
     *  @param[in] format    Format of the CSV file
     */
    inline int get_col_pos(
            std::string_view filename,
            std::string_view col_name,
            const CSVFormat &format) {
        CSVReader reader(filename, format);
        return reader.index_of(col_name);
    }

    /** Get basic information about a CSV file
     *  @include programs/csv_info.cpp
     */
    inline CSVFileInfo get_file_info(const std::string &filename) {
        CSVReader reader(filename);
        CSVFormat format = reader.get_format();
        for (auto it = reader.begin(); it != reader.end(); ++it);

        CSVFileInfo info = {
                filename,
                reader.get_col_names(),
                format.get_delim(),
                reader.n_rows(),
                reader.get_col_names().size()
        };

        return info;
    }
}  // namespace alkaid
