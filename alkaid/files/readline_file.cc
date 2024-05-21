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

#include <alkaid/files/readline_file.h>

namespace alkaid {

    collie::Status ReadlineFile::open(const std::string &file_path) {
        _file.open(file_path);
        if (!_file.is_open()) {
            return collie::Status(collie::StatusCode::IOError, "open file failed");
        }
        return collie::Status::ok_status();
    }

    collie::Result<std::string> ReadlineFile::readline() {
        std::string line;
        if (std::getline(_file, line)) {
            ++_line_num;
            return collie::Result<std::string>(line);
        }
        if (_file.eof()) {
            return collie::Status::unavailable("eof");
        }
        return collie::Result<std::string>(collie::Status(collie::StatusCode::IOError, "readline failed"));
    }

    void ReadlineFile::close() {
        _file.close();
    }

}  // namespace alkaid
