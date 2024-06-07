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

#include <fstream>
#include <turbo/utility/status.h>

namespace alkaid {

    class ReadlineFile {
    public:
        ReadlineFile() = default;
        ~ReadlineFile() = default;
        ReadlineFile(const ReadlineFile &) = delete;
        ReadlineFile &operator=(const ReadlineFile &) = delete;

        turbo::Status open(const std::string &file_path);

        [[nodiscard]] size_t lines() const { return _line_num; }

        turbo::Result<std::string> readline();

        void close();

    private:
        std::ifstream _file;
        size_t _line_num{0};

    };
}  // namespace alkaid
