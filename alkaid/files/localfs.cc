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

#include <alkaid/files/localfs.h>
#include <alkaid/files/sequential_read_file.h>
#include <alkaid/files/sequential_read_mmap_file.h>
#include <alkaid/files/sequential_write_file.h>
#include <alkaid/files/random_read_file.h>
#include <alkaid/files/random_read_mmap_file.h>
#include <alkaid/files/random_write_file.h>
#include <alkaid/files/temp_file.h>
#include <turbo/strings/substitute.h>
#include <alkaid/files/utility.h>

namespace alkaid {

    turbo::Result<std::shared_ptr<SequentialFileReader>> LocalFilesystem::create_sequential_read_file() {
        auto file = std::make_shared<SequentialReadFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<SequentialFileReader>> LocalFilesystem::create_sequential_read_mmap_file() {
        auto file = std::make_shared<SequentialReadMMapFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<RandomAccessFileReader>> LocalFilesystem::create_random_read_file() {
        auto file = std::make_shared<RandomReadFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<RandomAccessFileReader>> LocalFilesystem::create_random_read_mmap_file() {
        auto file = std::make_shared<RandomReadMMapFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<SequentialFileWriter>> LocalFilesystem::create_sequential_write_file() {
        auto file = std::make_shared<SequentialWriteFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<RandomAccessFileWriter>> LocalFilesystem::create_random_write_file() {
        auto file = std::make_shared<RandomWriteFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<TempFileWriter>> LocalFilesystem::create_temp_file() {
        auto file = std::make_shared<TempFile>();
        return file;
    }

    turbo::Status LocalFilesystem::read_file(const std::string &file_path, std::string *result) noexcept {
        return alkaid::read_file(file_path, result);
    }

    turbo::Status LocalFilesystem::write_file(const std::string &file_path, const std::string_view &content) noexcept {
        return alkaid::write_file(file_path, content);
    }

    turbo::Status LocalFilesystem::append_file(const std::string &file_path, const std::string_view &content) noexcept {
        return alkaid::append_file(file_path, content);
    }

    turbo::Status LocalFilesystem::list_files(const std::string_view &root_path, std::vector<std::string> &result,
                                              bool full_path) noexcept {
        return alkaid::list_files(root_path, result, full_path);
    }

    turbo::Status LocalFilesystem::list_directories(const std::string_view &root_path, std::vector<std::string> &result,
                                                    bool full_path) noexcept {
        return alkaid::list_directories(root_path, result, full_path);
    }

    LocalFilesystem *Filesystem::localfs() {
        static LocalFilesystem fs;
        return &fs;
    }

    turbo::Result<bool> LocalFilesystem::exists(const std::string_view &path) noexcept {
        std::error_code ec;
        auto r = alkaid::exists(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("test if exists error:$0", ec.message()));
        }
        return r;

    }

    turbo::Status LocalFilesystem::remove(const std::string_view &path) noexcept {
        std::error_code ec;
        alkaid::remove(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("remove file error:$0", ec.message()));
        }
        return turbo::OkStatus();

    }

    turbo::Status LocalFilesystem::remove_all(const std::string_view &path) noexcept {
        std::error_code ec;
        alkaid::remove_all(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("remove file error:$0", ec.message()));
        }
        return turbo::OkStatus();

    }

    turbo::Status LocalFilesystem::remove_if_exists(const std::string_view &path) noexcept {
        std::error_code ec;
        auto file_path = alkaid::FilePath(path);
        bool exists = alkaid::exists(file_path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("test if exists error:$0", ec.message()));
        }
        if (exists) {
            alkaid::remove(file_path, ec);
            if (ec) {
                return turbo::errno_to_status(ec.value(), turbo::substitute("remove file error:$0", ec.message()));
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::remove_all_if_exists(const std::string_view &path) noexcept {
        std::error_code ec;
        auto file_path = alkaid::FilePath(path);
        bool exists = alkaid::exists(file_path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("test if exists error:$0", ec.message()));
        }
        if (exists) {
            alkaid::remove_all(file_path, ec);
            if (ec) {
                return turbo::errno_to_status(ec.value(), turbo::substitute("remove file error:$0", ec.message()));
            }
        }
        return turbo::OkStatus();
    }

    turbo::Result<size_t> LocalFilesystem::file_size(const std::string_view &path) noexcept {
        std::error_code ec;
        auto r = alkaid::file_size(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("get file size error:$0", ec.message()));
        }
        return r;
    }

    turbo::Result<turbo::Time> LocalFilesystem::last_modified_time(const std::string_view &path) noexcept {
        std::error_code ec;
        auto r = alkaid::last_write_time(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(),
                                          turbo::substitute("get last modified time error:$0", ec.message()));
        }
        return r;
    }

    turbo::Status LocalFilesystem::rename(const std::string_view &old_path, const std::string_view &new_path) noexcept {
        std::error_code ec;
        alkaid::rename(old_path, new_path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("rename file error:$0", ec.message()));
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::file_resize(const std::string_view &path, size_t size) noexcept {
        std::error_code ec;
        alkaid::resize_file(path, size, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("resize file error:$0", ec.message()));
        }
        return turbo::OkStatus();
    }

    turbo::Status
    LocalFilesystem::copy_file(const std::string_view &src_path, const std::string_view &dst_path) noexcept {
        std::error_code ec;
        if (alkaid::is_directory(src_path, ec)) {
            return turbo::invalid_argument_error(turbo::substitute("source path is a directory:$0", src_path));
        }
        alkaid::copy_file(src_path, dst_path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("copy file error:$0", ec.message()));
        }
        return turbo::OkStatus();
    }

    turbo::Result<std::string> LocalFilesystem::temp_directory_path() noexcept {
        std::error_code ec;
        auto r = alkaid::temp_directory_path(ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(),
                                          turbo::substitute("get temp directory path error:$0", ec.message()));
        }
        return r.string();
    }

    turbo::Status LocalFilesystem::create_directory(const std::string_view &path) noexcept {
        std::error_code ec;
        alkaid::create_directory(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("create directory error:$0", ec.message()));
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::create_directories(const std::string_view &path) noexcept {
        std::error_code ec;
        alkaid::create_directories(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("create directories error:$0", ec.message()));
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::copy_directory(const std::string_view &src_path, const std::string_view &dst_path, CopyOptions opt) noexcept {
        std::error_code ec;
        auto options = static_cast<alkaid::CopyOptions>(opt);
        alkaid::copy(src_path, dst_path, options, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("copy directory error:$0", ec.message()));
        }
        return turbo::OkStatus();

    }

}  // namespace alkaid