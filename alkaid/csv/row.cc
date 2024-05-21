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

#include <alkaid/csv/row.h>
#include <alkaid/csv/reader.h>
#include <alkaid/csv/csv.h>
namespace alkaid {
    namespace internals {
        /*!
         @brief calculates the extra space to escape a JSON string

         @param[in] s  the string to escape
         @return the number of characters required to escape string @a s

         @complexity Linear in the length of string @a s.
        */
        static std::size_t json_extra_space(std::string_view &s) noexcept {
            std::size_t result = 0;


            for (const auto &c: s) {
                switch (c) {
                    case '"':
                    case '\\':
                    case '\b':
                    case '\f':
                    case '\n':
                    case '\r':
                    case '\t': {
                        // from c (1 byte) to \x (2 bytes)
                        result += 1;
                        break;
                    }


                    default: {
                        if (c >= 0x00 && c <= 0x1f) {
                            // from c (1 byte) to \uxxxx (6 bytes)
                            result += 5;
                        }
                        break;
                    }
                }
            }


            return result;
        }

        std::string json_escape_string(std::string_view s) noexcept {
            const auto space = json_extra_space(s);
            if (space == 0) {
                return std::string(s);
            }

            // create a result string of necessary size
            size_t result_size = s.size() + space;
            std::string result(result_size, '\\');
            std::size_t pos = 0;

            for (const auto &c: s) {
                switch (c) {
                    // quotation mark (0x22)
                    case '"': {
                        result[pos + 1] = '"';
                        pos += 2;
                        break;
                    }


                    // reverse solidus (0x5c)
                    case '\\': {
                        // nothing to change
                        pos += 2;
                        break;
                    }


                    // backspace (0x08)
                    case '\b': {
                        result[pos + 1] = 'b';
                        pos += 2;
                        break;
                    }


                    // formfeed (0x0c)
                    case '\f': {
                        result[pos + 1] = 'f';
                        pos += 2;
                        break;
                    }


                    // newline (0x0a)
                    case '\n': {
                        result[pos + 1] = 'n';
                        pos += 2;
                        break;
                    }


                // carriage return (0x0d)
                    case '\r': {
                        result[pos + 1] = 'r';
                        pos += 2;
                        break;
                    }


                    // horizontal tab (0x09)
                    case '\t': {
                        result[pos + 1] = 't';
                        pos += 2;
                        break;
                    }


                    default: {
                        if (c >= 0x00 && c <= 0x1f) {
                            // print character c as \uxxxx
                            snprintf(&result[pos + 1], result_size - pos - 1, "u%04x", int(c));
                            pos += 6;
                            // overwrite trailing null character
                            result[pos] = '\\';
                        } else {
                        // all other characters are added as-is
                            result[pos++] = c;
                        }
                        break;
                    }
                }
            }

            return result;
        }
    }

    /** Convert a CSV row to a JSON object, i.e.
     *  `{"col1":"value1","col2":"value2"}`
     *
     *  @note All strings are properly escaped. Numeric values are not quoted.
     *  @param[in] subset A subset of columns to contain in the JSON.
     *                    Leave empty for original columns.
     */
    std::string CSVRow::to_json(const std::vector<std::string> &subset) const {
        std::vector<std::string> col_names = subset;
        if (subset.empty()) {
            col_names = this->data ? this->get_col_names() : std::vector<std::string>({});
        }

        const size_t _n_cols = col_names.size();
        std::string ret = "{";

        for (size_t i = 0; i < _n_cols; i++) {
            auto &col = col_names[i];
            auto field = this->operator[](col);

            // TODO: Possible performance enhancements by caching escaped column names
            ret += '"' + internals::json_escape_string(col) + "\":";

            // Add quotes around strings but not numbers
            if (field.is_num())
                ret += internals::json_escape_string(field.get<std::string_view>());
            else
                ret += '"' + internals::json_escape_string(field.get<std::string_view>()) + '"';

            // Do not add comma after last string
            if (i + 1 < _n_cols)
                ret += ',';
        }

        ret += '}';
        return ret;
    }

    /** Convert a CSV row to a JSON array, i.e.
     *  `["value1","value2",...]`
     *
     *  @note All strings are properly escaped. Numeric values are not quoted.
     *  @param[in] subset A subset of columns to contain in the JSON.
     *                    Leave empty for all columns.
     */
    std::string CSVRow::to_json_array(const std::vector<std::string> &subset) const {
        std::vector<std::string> col_names = subset;
        if (subset.empty())
            col_names = this->data ? this->get_col_names() : std::vector<std::string>({});

        const size_t _n_cols = col_names.size();
        std::string ret = "[";

        for (size_t i = 0; i < _n_cols; i++) {
            auto field = this->operator[](col_names[i]);

            // Add quotes around strings but not numbers
            if (field.is_num())
                ret += internals::json_escape_string(field.get<std::string_view>());
            else
                ret += '"' + internals::json_escape_string(field.get<std::string_view>()) + '"';

            // Do not add comma after last string
            if (i + 1 < _n_cols)
                ret += ',';
        }

        ret += ']';
        return ret;
    }
}  // namespace alkaid