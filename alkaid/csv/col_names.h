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

#include <memory>
#include <unordered_map>
#include <string>
#include <vector>
#include <alkaid/csv/defines.h>

namespace alkaid::internals {

    struct ColNames;
    using ColNamesPtr = std::shared_ptr<ColNames>;

    /** @struct ColNames
         *  A data structure for handling column name information.
         *
         *  These are created by CSVReader and passed (via smart pointer)
         *  to CSVRow objects it creates, thus
         *  allowing for indexing by column name.
         */
    struct ColNames {
    public:
        ColNames() = default;

        ColNames(const std::vector <std::string> &names) {
            set_col_names(names);
        }

        std::vector <std::string> get_col_names() const;

        void set_col_names(const std::vector <std::string> &);

        int index_of(std::string_view) const;

        bool empty() const noexcept { return this->col_names.empty(); }

        size_t size() const noexcept;

    private:
        std::vector <std::string> col_names;
        std::unordered_map <std::string, size_t> col_pos;
    };

    inline std::vector <std::string> ColNames::get_col_names() const {
        return this->col_names;
    }

    inline void ColNames::set_col_names(const std::vector <std::string> &cnames) {
        this->col_names = cnames;

        for (size_t i = 0; i < cnames.size(); i++) {
            this->col_pos[cnames[i]] = i;
        }
    }

    inline int ColNames::index_of(std::string_view col_name) const {
        auto pos = this->col_pos.find(col_name.data());
        if (pos != this->col_pos.end())
            return (int) pos->second;

        return CSV_NOT_FOUND;
    }

    inline size_t ColNames::size() const noexcept {
        return this->col_names.size();
    }


}  // namespace alkaid::internals
