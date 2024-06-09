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
#include <alkaid/files/local/sequential_read_file.h>
#include <alkaid/files/local/sequential_read_mmap_file.h>
#include <alkaid/files/local/sequential_write_file.h>
#include <alkaid/files/local/random_read_file.h>
#include <alkaid/files/local/random_read_mmap_file.h>
#include <alkaid/files/local/random_write_file.h>
#include <alkaid/files/local/temp_file.h>
#include <turbo/strings/substitute.h>

namespace alkaid {

    turbo::Result<std::shared_ptr<SequentialFileReader>> LocalFilesystem::create_sequential_read_file() {
        auto file = std::make_shared<lfs::SequentialReadFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<SequentialFileReader>> LocalFilesystem::create_sequential_read_mmap_file() {
        auto file = std::make_shared<lfs::SequentialReadMMapFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<RandomAccessFileReader>> LocalFilesystem::create_random_read_file() {
        auto file = std::make_shared<lfs::RandomReadFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<RandomAccessFileReader>> LocalFilesystem::create_random_read_mmap_file() {
        auto file = std::make_shared<lfs::RandomReadMMapFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<SequentialFileWriter>> LocalFilesystem::create_sequential_write_file() {
        auto file = std::make_shared<lfs::SequentialWriteFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<RandomAccessFileWriter>> LocalFilesystem::create_random_write_file() {
        auto file = std::make_shared<lfs::RandomWriteFile>();
        return file;
    }

    turbo::Result<std::shared_ptr<TempFileWriter>> LocalFilesystem::create_temp_file() {
        auto file = std::make_shared<lfs::TempFile>();
        return file;
    }

    turbo::Status LocalFilesystem::read_file(const std::string &file_path, std::string *result) noexcept {
        lfs::SequentialReadFile file;
        auto rs = file.open(file_path, std::any{}, FileEventListener{});
        if (!rs.ok()) {
            return rs;
        }
        auto rsize = file.size();
        if (!rsize.ok()) {
            return rsize.status();
        }
        auto r = file.read(result, rsize.value());
        if (!r.ok()) {
            return r.status();
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::write_file(const std::string &file_path, const std::string_view &content) noexcept {
        lfs::SequentialWriteFile file;
        auto rs = file.open(file_path, lfs::kDefaultTruncateWriteOption, FileEventListener{});
        if (!rs.ok()) {
            return rs;
        }

        rs = file.append(content);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::append_file(const std::string &file_path, const std::string_view &content) noexcept {
        lfs::SequentialWriteFile file;
        auto rs = file.open(file_path, lfs::kDefaultAppendWriteOption, FileEventListener{});
        if (!rs.ok()) {
            return rs;
        }

        rs = file.append(content);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::list_files(const std::string_view &root_path, std::vector<std::string> &result,
                                              bool full_path) noexcept {
        std::error_code ec;
        ghc::filesystem::directory_iterator itr(root_path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("open directory error:$0", ec.message()));
        }
        ghc::filesystem::directory_iterator end;
        for (; itr != end; ++itr) {
            if (!itr->is_directory(ec)) {
                if (ec) {
                    return turbo::errno_to_status(ec.value(),
                                                  turbo::substitute("test if file error: $0", ec.message()));
                }
                if (full_path) {
                    result.emplace_back(itr->path().string());
                } else {
                    result.emplace_back(itr->path().filename());
                }
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::list_directories(const std::string_view &root_path, std::vector<std::string> &result,
                                                    bool full_path) noexcept {
        std::error_code ec;
        ghc::filesystem::directory_iterator itr(root_path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("open directory error: $0", ec.message()));
        }
        ghc::filesystem::directory_iterator end;
        for (; itr != end; ++itr) {
            if (itr->is_directory(ec)) {
                if (ec) {
                    return turbo::errno_to_status(ec.value(),
                                                  turbo::substitute("test if directory error:$0", ec.message()));
                }
                if (full_path) {
                    result.emplace_back(itr->path().string());
                } else {
                    result.emplace_back(itr->path().filename());
                }
            }
        }
        return turbo::OkStatus();
    }

    LocalFilesystem *Filesystem::localfs() {
        static LocalFilesystem fs;
        return &fs;
    }

    turbo::Result<bool> LocalFilesystem::exists(const std::string_view &path) noexcept {
        std::error_code ec;
        auto r = ghc::filesystem::exists(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("test if exists error:$0", ec.message()));
        }
        return r;

    }

    turbo::Status LocalFilesystem::remove(const std::string_view &path) noexcept {
        std::error_code ec;
        ghc::filesystem::remove(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("remove file error:$0", ec.message()));
        }
        return turbo::OkStatus();

    }

    turbo::Status LocalFilesystem::remove_all(const std::string_view &path) noexcept {
        std::error_code ec;
        ghc::filesystem::remove_all(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("remove file error:$0", ec.message()));
        }
        return turbo::OkStatus();

    }

    turbo::Status LocalFilesystem::remove_if_exists(const std::string_view &path) noexcept {
        std::error_code ec;
        auto file_path = ghc::filesystem::path(path);
        bool exists = ghc::filesystem::exists(file_path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("test if exists error:$0", ec.message()));
        }
        if (exists) {
            ghc::filesystem::remove(file_path, ec);
            if (ec) {
                return turbo::errno_to_status(ec.value(), turbo::substitute("remove file error:$0", ec.message()));
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::remove_all_if_exists(const std::string_view &path) noexcept {
        std::error_code ec;
        auto file_path = ghc::filesystem::path(path);
        bool exists = ghc::filesystem::exists(file_path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("test if exists error:$0", ec.message()));
        }
        if (exists) {
            ghc::filesystem::remove_all(file_path, ec);
            if (ec) {
                return turbo::errno_to_status(ec.value(), turbo::substitute("remove file error:$0", ec.message()));
            }
        }
        return turbo::OkStatus();
    }

    turbo::Result<size_t> LocalFilesystem::file_size(const std::string_view &path) noexcept {
        std::error_code ec;
        auto r = ghc::filesystem::file_size(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("get file size error:$0", ec.message()));
        }
        return r;
    }

    turbo::Result<turbo::Time> LocalFilesystem::last_modified_time(const std::string_view &path) noexcept {
        std::error_code ec;
        auto r = ghc::filesystem::last_write_time(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(),
                                          turbo::substitute("get last modified time error:$0", ec.message()));
        }
        return turbo::Time::from_chrono(r);
    }

    turbo::Status LocalFilesystem::rename(const std::string_view &old_path, const std::string_view &new_path) noexcept {
        std::error_code ec;
        ghc::filesystem::rename(old_path, new_path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("rename file error:$0", ec.message()));
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::file_resize(const std::string_view &path, size_t size) noexcept {
        std::error_code ec;
        ghc::filesystem::resize_file(path, size, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("resize file error:$0", ec.message()));
        }
        return turbo::OkStatus();
    }

    turbo::Status
    LocalFilesystem::copy_file(const std::string_view &src_path, const std::string_view &dst_path) noexcept {
        std::error_code ec;
        if (ghc::filesystem::is_directory(src_path, ec)) {
            return turbo::invalid_argument_error(turbo::substitute("source path is a directory:$0", src_path));
        }
        ghc::filesystem::copy_file(src_path, dst_path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("copy file error:$0", ec.message()));
        }
        return turbo::OkStatus();
    }

    turbo::Result<std::string> LocalFilesystem::temp_directory_path() noexcept {
        std::error_code ec;
        auto r = ghc::filesystem::temp_directory_path(ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(),
                                          turbo::substitute("get temp directory path error:$0", ec.message()));
        }
        return r.string();
    }

    turbo::Status LocalFilesystem::create_directory(const std::string_view &path) noexcept {
        std::error_code ec;
        ghc::filesystem::create_directory(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("create directory error:$0", ec.message()));
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::create_directories(const std::string_view &path) noexcept {
        std::error_code ec;
        ghc::filesystem::create_directories(path, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("create directories error:$0", ec.message()));
        }
        return turbo::OkStatus();
    }

    turbo::Status LocalFilesystem::copy_directory(const std::string_view &src_path, const std::string_view &dst_path, CopyOptions opt) noexcept {
        std::error_code ec;
        auto options = static_cast<ghc::filesystem::copy_options>(opt);
        ghc::filesystem::copy(src_path, dst_path, options, ec);
        if (ec) {
            return turbo::errno_to_status(ec.value(), turbo::substitute("copy directory error:$0", ec.message()));
        }
        return turbo::OkStatus();

    }

}  // namespace alkaid