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

#include <alkaid/files/fwd.h>
#include <alkaid/files/sequential_write_file.h>

namespace alkaid {

    // Create a temporary file in current directory, which will be deleted when
    // corresponding temp_file object destructs, typically for unit testing.
    //
    // Usage:
    //   {
    //      temp_file tmpfile;           // A temporay file shall be created
    //      tmpfile.save("some text");  // Write into the temporary file
    //   }
    //   // The temporary file shall be removed due to destruction of tmpfile

    class TempFile : public TempFileWriter {
    public:

        // Create a temporary file in current directory. If |ext| is given,
        // filename will be temp_file_XXXXXX.|ext|, temp_file_XXXXXX otherwise.
        // If temporary file cannot be created, all save*() functions will
        // return -1. If |ext| is too long, filename will be truncated.
        TempFile() = default;

        explicit TempFile(const FileEventListener &listener);

        // The temporary file is removed in destructor.
        ~TempFile() {
            _file.close();
        }

        [[nodiscard]] virtual turbo::Status open(std::string_view prefix = kDefaultTempFilePrefix, std::string_view ext ="", size_t bits = 6) noexcept override;

        turbo::Status write(const void *buf, size_t count) override;

        // Save binary data |buf| (|count| bytes) to file, overwriting existing file.
        // Returns 0 when successful, -1 otherwise.

        // Get name of the temporary file.
        std::string path() const override { return _file_path; }

        [[nodiscard]] turbo::Status write(std::string_view buff) override;

        [[nodiscard]] turbo::Status flush() override { return _file.flush(); }

        [[nodiscard]] turbo::Status truncate(size_t size) override { return _file.truncate(size); }

        [[nodiscard]] turbo::Result<size_t> size() const override { return _file.size(); }

        void close() override { _file.close(); }


    private:

        std::string generate_temp_file_name(std::string_view prefix, std::string_view ext, size_t bits);
        std::string _file_path;
        SequentialWriteFile _file;
        bool         _ever_opened{false};
    };

    /// inlined implementations

    [[nodiscard]] inline turbo::Status TempFile::write(std::string_view buff) {
        return write(buff.data(), buff.size());
    }


} // namespace alkaid
