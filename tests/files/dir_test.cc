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
// Created by jeff on 24-6-10.
//

#include <gtest/gtest.h>
#include <random>
#include <alkaid/files/filesystem.h>
#include "helper.h"

TEST(dir, directory_entry) {
    TemporaryDirectory t;
    std::error_code ec;
    auto de = alkaid::DirectoryEntry(t.path());
    ASSERT_TRUE(de.path() == t.path());
    ASSERT_TRUE((alkaid::FilePath) de == t.path());
    RESULT_OK_AND_TRUE(de.exists());
    RESULT_OK_AND_FALSE(de.is_block_file());
    RESULT_OK_AND_FALSE(de.is_character_file());
    RESULT_OK_AND_TRUE(de.is_directory());
    RESULT_OK_AND_FALSE(de.is_fifo());
    RESULT_OK_AND_FALSE(de.is_other());
    RESULT_OK_AND_FALSE(de.is_regular_file());
    RESULT_OK_AND_FALSE(de.is_socket());
    RESULT_OK_AND_FALSE(de.is_symlink());
    ASSERT_TRUE(de.status().value().type() == alkaid::FileType::directory);
    ec.clear();
    ASSERT_TRUE(de.status(ec).type() == alkaid::FileType::directory);
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(de.refresh().ok());
    alkaid::DirectoryEntry none;
    ASSERT_TRUE(!none.refresh().ok());
    ec.clear();
    ASSERT_NO_THROW(none.refresh(ec));
    ASSERT_TRUE(ec);
    ASSERT_TRUE(!de.assign("").ok());
    ec.clear();
    ASSERT_NO_THROW(de.assign("", ec));
    ASSERT_TRUE(ec);
    auto now = turbo::Time::current_time();
    generateFile(t.path() / "foo", 1234);
    ASSERT_TRUE(de.assign(t.path() / "foo").ok());
    ASSERT_NO_THROW(de.assign(t.path() / "foo", ec));
    ASSERT_TRUE(!ec);
    de = alkaid::DirectoryEntry(t.path() / "foo");
    ASSERT_TRUE(de.path() == t.path() / "foo");
    RESULT_OK_AND_TRUE(de.exists());
    ASSERT_TRUE(de.exists(ec));
    ASSERT_TRUE(!ec);
    RESULT_OK_AND_FALSE(de.is_block_file());
    ASSERT_TRUE(!de.is_block_file(ec));
    ASSERT_TRUE(!ec);
    RESULT_OK_AND_FALSE(de.is_character_file());
    ASSERT_TRUE(!de.is_character_file(ec));
    ASSERT_TRUE(!ec);
    RESULT_OK_AND_FALSE(de.is_directory());
    ASSERT_TRUE(!de.is_directory(ec));
    ASSERT_TRUE(!ec);
    RESULT_OK_AND_FALSE(de.is_fifo());
    ASSERT_TRUE(!de.is_fifo(ec));
    ASSERT_TRUE(!ec);
    RESULT_OK_AND_FALSE(de.is_other());
    ASSERT_TRUE(!de.is_other(ec));
    ASSERT_TRUE(!ec);
    RESULT_OK_AND_TRUE(de.is_regular_file());
    ASSERT_TRUE(de.is_regular_file(ec));
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(!de.is_socket().value());
    ASSERT_TRUE(!de.is_socket(ec));
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(!de.is_symlink().value());
    ASSERT_TRUE(!de.is_symlink(ec));
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(de.file_size().value() == 1234);
    ASSERT_TRUE(de.file_size(ec) == 1234);
    ASSERT_TRUE(turbo::Duration::to_seconds(de.last_write_time().value() - now) < 3);
    ec.clear();
    ASSERT_TRUE(turbo::Duration::to_seconds(de.last_write_time(ec) - now) < 3);
    ASSERT_TRUE(!ec);
#ifndef ALKAID_OS_WEB
    ASSERT_TRUE(de.hard_link_count().value() == 1);
    ASSERT_TRUE(de.hard_link_count(ec) == 1);
    ASSERT_TRUE(!ec);
#endif
    ASSERT_FALSE(de.replace_filename("bar").ok());
    ASSERT_TRUE(de.replace_filename("foo").ok());
    ec.clear();
    ASSERT_NO_THROW(de.replace_filename("bar", ec));
    ASSERT_TRUE(ec);
    auto de2none = alkaid::DirectoryEntry();
    ec.clear();
#ifndef ALKAID_OS_WEB
    ASSERT_TRUE(de2none.hard_link_count(ec) == static_cast<uintmax_t>(-1));
    ASSERT_FALSE(de2none.hard_link_count().ok());
    ASSERT_TRUE(ec);
#endif
    ec.clear();
    ASSERT_NO_THROW(de2none.last_write_time(ec));
    ASSERT_FALSE(de2none.last_write_time().ok());
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_FALSE(de2none.file_size().ok());
    ASSERT_TRUE(de2none.file_size(ec) == static_cast<uintmax_t>(-1));
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_TRUE(de2none.status().value().type() == alkaid::FileType::not_found);
    ASSERT_TRUE(de2none.status(ec).type() == alkaid::FileType::not_found);
    ASSERT_TRUE(ec);
    generateFile(t.path() / "a");
    generateFile(t.path() / "b");
    auto d1 = alkaid::DirectoryEntry(t.path() / "a");
    auto d2 = alkaid::DirectoryEntry(t.path() / "b");
    ASSERT_TRUE(d1 < d2);
    ASSERT_TRUE(!(d2 < d1));
    ASSERT_TRUE(d1 <= d2);
    ASSERT_TRUE(!(d2 <= d1));
    ASSERT_TRUE(d2 > d1);
    ASSERT_TRUE(!(d1 > d2));
    ASSERT_TRUE(d2 >= d1);
    ASSERT_TRUE(!(d1 >= d2));
    ASSERT_TRUE(d1 != d2);
    ASSERT_TRUE(!(d2 != d2));
    ASSERT_TRUE(d1 == d1);
    ASSERT_TRUE(!(d1 == d2));
    if (is_symlink_creation_supported()) {
        auto rs = alkaid::create_symlink(t.path() / "nonexistent", t.path() / "broken");
        ASSERT_TRUE(rs.ok());
        for (auto d3: alkaid::DirectoryIterator(t.path())) {
            ASSERT_TRUE(d3.symlink_status().ok());
            ASSERT_TRUE(d3.status().ok());
            ASSERT_TRUE(d3.refresh().ok());
        }
        alkaid::DirectoryEntry entry(t.path() / "broken");
        ASSERT_TRUE(entry.refresh().ok());
    }
}

TEST(DirectoryIterator, DirectoryIterator) {
    {
        LOG(INFO) << "0";
        TemporaryDirectory t;
        ASSERT_TRUE(alkaid::DirectoryIterator(t.path()) == alkaid::DirectoryIterator());
        generateFile(t.path() / "test", 1234);
        ASSERT_TRUE(alkaid::DirectoryIterator(t.path()) != alkaid::DirectoryIterator());
        auto iter = alkaid::DirectoryIterator(t.path());
        alkaid::DirectoryIterator iter2(iter);
        alkaid::DirectoryIterator iter3, iter4;
        iter3 = iter;
        LOG(INFO) << "1";
        ASSERT_TRUE(iter->path().filename() == "test");
        ASSERT_TRUE(iter2->path().filename() == "test");
        ASSERT_TRUE(iter3->path().filename() == "test");
        iter4 = std::move(iter3);
        ASSERT_TRUE(iter4->path().filename() == "test");
        ASSERT_TRUE(iter->path() == t.path() / "test");
        RESULT_OK_AND_FALSE(iter->is_symlink());
        RESULT_OK_AND_TRUE(iter->is_regular_file());
        RESULT_OK_AND_FALSE(iter->is_directory());
        LOG(INFO) << "2";
        ASSERT_TRUE(iter->file_size().value() == 1234);
        LOG(INFO) << "3";
        ASSERT_TRUE(++iter == alkaid::DirectoryIterator());
        ASSERT_FALSE(alkaid::DirectoryIterator(t.path() / "non-existing").status().ok());
        int cnt = 0;
        for (auto de: alkaid::DirectoryIterator(t.path())) {
            ++cnt;
        }
        ASSERT_TRUE(cnt == 1);
    }
    if (is_symlink_creation_supported()) {
        TemporaryDirectory t;
        alkaid::FilePath td = t.path() / "testdir";
        ASSERT_TRUE(alkaid::DirectoryIterator(t.path()) == alkaid::DirectoryIterator());
        generateFile(t.path() / "test", 1234);
        ASSERT_TRUE(alkaid::create_directory(td).ok());
        ASSERT_TRUE(alkaid::create_symlink(t.path() / "test", td / "testlink").ok());
        std::error_code ec;
        ASSERT_TRUE(alkaid::DirectoryIterator(td) != alkaid::DirectoryIterator());
        auto iter = alkaid::DirectoryIterator(td);
        ASSERT_TRUE(iter->path().filename() == "testlink");
        ASSERT_TRUE(iter->path() == td / "testlink");
        ASSERT_TRUE(iter->is_symlink().value()) << iter->is_symlink().value();
        ASSERT_TRUE(iter->is_regular_file().value());
        ASSERT_TRUE(!iter->is_directory().value());
        ASSERT_TRUE(iter->file_size().value() == 1234);
        ASSERT_TRUE(++iter == alkaid::DirectoryIterator());
    }
    {
        // Issue #8: check if resources are freed when iterator reaches end()
        TemporaryDirectory t(TempOpt::change_path);
        auto p = alkaid::FilePath("test/");
        ASSERT_TRUE(alkaid::create_directory(p).ok());
        auto iter = alkaid::DirectoryIterator(p);
        while (iter != alkaid::DirectoryIterator()) {
            ++iter;
        }
        ASSERT_TRUE(alkaid::remove_all(p).value() == 1);
        ASSERT_TRUE(alkaid::create_directory(p).ok());
    }
}


TEST(rec_dir, itr) {
    {
        auto iter = alkaid::RecursiveDirectoryIterator(".");
        ASSERT_TRUE(iter.pop().ok());
        ASSERT_TRUE(iter == alkaid::RecursiveDirectoryIterator());
    }
    {
        TemporaryDirectory t;
        ASSERT_TRUE(alkaid::RecursiveDirectoryIterator(t.path()) == alkaid::RecursiveDirectoryIterator());
        generateFile(t.path() / "test", 1234);
        ASSERT_TRUE(alkaid::RecursiveDirectoryIterator(t.path()) != alkaid::RecursiveDirectoryIterator());
        auto iter = alkaid::RecursiveDirectoryIterator(t.path());
        ASSERT_TRUE(iter->path().filename() == "test");
        ASSERT_TRUE(iter->path() == t.path() / "test");
        ASSERT_TRUE(!iter->is_symlink().value());
        ASSERT_TRUE(iter->is_regular_file().value());
        ASSERT_TRUE(!iter->is_directory().value());
        ASSERT_TRUE(iter->file_size().value() == 1234);
        ASSERT_TRUE(++iter == alkaid::RecursiveDirectoryIterator());
    }

    {
        TemporaryDirectory t;
        alkaid::FilePath td = t.path() / "testdir";
        ASSERT_TRUE(alkaid::create_directories(td).ok());
        generateFile(td / "test", 1234);
        ASSERT_TRUE(alkaid::RecursiveDirectoryIterator(t.path()) != alkaid::RecursiveDirectoryIterator());
        auto iter = alkaid::RecursiveDirectoryIterator(t.path());

        ASSERT_TRUE(iter->path().filename() == "testdir");
        ASSERT_TRUE(iter->path() == td);
        ASSERT_TRUE(!iter->is_symlink().value());
        ASSERT_TRUE(!iter->is_regular_file().value());
        ASSERT_TRUE(iter->is_directory().value());

        ASSERT_TRUE(++iter != alkaid::RecursiveDirectoryIterator());

        ASSERT_TRUE(iter->path().filename() == "test");
        ASSERT_TRUE(iter->path() == td / "test");
        ASSERT_TRUE(!iter->is_symlink().value());
        ASSERT_TRUE(iter->is_regular_file().value());
        ASSERT_TRUE(!iter->is_directory().value());
        ASSERT_TRUE(iter->file_size().value() == 1234);

        ASSERT_TRUE(++iter == alkaid::RecursiveDirectoryIterator());
    }
    {
        TemporaryDirectory t;
        std::error_code ec;
        ASSERT_TRUE(alkaid::RecursiveDirectoryIterator(t.path(), alkaid::DirectoryOptions::none) ==
                    alkaid::RecursiveDirectoryIterator());
        ASSERT_TRUE(alkaid::RecursiveDirectoryIterator(t.path(), alkaid::DirectoryOptions::none, ec) ==
                    alkaid::RecursiveDirectoryIterator());
        ASSERT_TRUE(!ec);
        ASSERT_TRUE(alkaid::RecursiveDirectoryIterator(t.path(), ec) == alkaid::RecursiveDirectoryIterator());
        ASSERT_TRUE(!ec);
        generateFile(t.path() / "test");
        alkaid::RecursiveDirectoryIterator rd1(t.path());
        ASSERT_TRUE(alkaid::RecursiveDirectoryIterator(rd1) != alkaid::RecursiveDirectoryIterator());
        alkaid::RecursiveDirectoryIterator rd2(t.path());
        ASSERT_TRUE(alkaid::RecursiveDirectoryIterator(std::move(rd2)) != alkaid::RecursiveDirectoryIterator());
        alkaid::RecursiveDirectoryIterator rd3(t.path(), alkaid::DirectoryOptions::skip_permission_denied);
        ASSERT_TRUE(rd3.options() == alkaid::DirectoryOptions::skip_permission_denied);
        alkaid::RecursiveDirectoryIterator rd4;
        rd4 = std::move(rd3);
        ASSERT_TRUE(rd4 != alkaid::RecursiveDirectoryIterator());
        ASSERT_FALSE(++rd4);
        ASSERT_TRUE(rd4->status().ok());
        ASSERT_TRUE(rd4 == alkaid::RecursiveDirectoryIterator());
        alkaid::RecursiveDirectoryIterator rd5;
        rd5 = rd4;
    }
    {
        TemporaryDirectory t(TempOpt::change_path);
        generateFile("a");
        ASSERT_TRUE(alkaid::create_directory("d1").ok());
        ASSERT_TRUE(alkaid::create_directory("d1/d2").ok());
        generateFile("d1/b");
        generateFile("d1/c");
        generateFile("d1/d2/d");
        generateFile("e");
        auto iter = alkaid::RecursiveDirectoryIterator(".");
        std::multimap<std::string, int> result;
        while (iter != alkaid::RecursiveDirectoryIterator()) {
            result.insert(std::make_pair(iter->path().generic_string(), iter.depth()));
            ++iter;
        }
        std::stringstream os;
        for (auto p: result) {
            os << "[" << p.first << "," << p.second << "],";
        }
        ASSERT_TRUE(os.str() == "[./a,0],[./d1,0],[./d1/b,1],[./d1/c,1],[./d1/d2,1],[./d1/d2/d,2],[./e,0],");
    }
    {
        TemporaryDirectory t(TempOpt::change_path);
        generateFile("a");
        ASSERT_TRUE(alkaid::create_directory("d1").ok());
        ASSERT_TRUE(alkaid::create_directory("d1/d2").ok());
        generateFile("d1/b");
        generateFile("d1/c");
        generateFile("d1/d2/d");
        generateFile("e");
        std::multiset<std::string> result;
        for (auto &de: alkaid::RecursiveDirectoryIterator(".")) {
            ASSERT_TRUE(de);
            result.insert(de.path().generic_string());
        }
        std::stringstream os;
        for (auto& p: result) {
            os << p << ",";
        }
        ASSERT_TRUE(os.str() == "./a,./d1,./d1/b,./d1/c,./d1/d2,./d1/d2/d,./e,");
    }
    {
        TemporaryDirectory t(TempOpt::change_path);
        generateFile("a");
        ASSERT_TRUE(alkaid::create_directory("d1").ok());
        ASSERT_TRUE(alkaid::create_directory("d1/d2").ok());
        generateFile("d1/d2/b");
        generateFile("e");
        auto iter = alkaid::RecursiveDirectoryIterator(".");
        std::multimap<std::string, int> result;
        while (iter != alkaid::RecursiveDirectoryIterator()) {
            result.insert(std::make_pair(iter->path().generic_string(), iter.depth()));
            if (iter->path() == "./d1/d2") {
                iter.disable_recursion_pending();
            }
            ++iter;
        }
        std::stringstream os;
        for (auto p: result) {
            os << "[" << p.first << "," << p.second << "],";
        }
        ASSERT_TRUE(os.str() == "[./a,0],[./d1,0],[./d1/d2,1],[./e,0],");
    }
    {
        TemporaryDirectory t(TempOpt::change_path);
        generateFile("a");
        ASSERT_TRUE(alkaid::create_directory("d1").ok());
        ASSERT_TRUE(alkaid::create_directory("d1/d2").ok());
        generateFile("d1/d2/b");
        generateFile("e");
        auto iter = alkaid::RecursiveDirectoryIterator(".");
        std::multimap<std::string, int> result;
        while (iter != alkaid::RecursiveDirectoryIterator()) {
            result.insert(std::make_pair(iter->path().generic_string(), iter.depth()));
            if (iter->path() == "./d1/d2") {
                ASSERT_TRUE(iter.pop().ok());
            } else {
                ++iter;
            }
        }
        std::stringstream os;
        for (auto& p: result) {
            os << "[" << p.first << "," << p.second << "],";
        }
        ASSERT_TRUE(os.str() == "[./a,0],[./d1,0],[./d1/d2,1],[./e,0],");
    }
    if (is_symlink_creation_supported()) {
        TemporaryDirectory t(TempOpt::change_path);
        ASSERT_TRUE(alkaid::create_directory("d1").ok());
        generateFile("d1/a");
        ASSERT_TRUE(alkaid::create_directory("d2").ok());
        generateFile("d2/b");
        ASSERT_TRUE(alkaid::create_directory_symlink("../d1", "d2/ds1").ok());
        ASSERT_TRUE(alkaid::create_directory_symlink("d3", "d2/ds2").ok());
        std::multiset<std::string> result;
        ASSERT_NO_THROW([&]() {
            for (const auto &de: alkaid::RecursiveDirectoryIterator("d2",
                                                                    alkaid::DirectoryOptions::follow_directory_symlink)) {
                if(!de) {
                    throw std::runtime_error("Invalid directory entry");
                }
                result.insert(de.path().generic_string());
            }
        }());
        std::stringstream os;
        for (const auto &p: result) {
            os << p << ",";
        }
        ASSERT_TRUE(os.str() == "d2/b,d2/ds1,d2/ds1/a,d2/ds2,")<<os.str();
        os.str("");
        result.clear();
        ASSERT_NO_THROW([&]() {
            for (const auto &de: alkaid::RecursiveDirectoryIterator("d2")) {
                if(!de) {
                    throw std::runtime_error("Invalid directory entry");
                }
                result.insert(de.path().generic_string());
            }
        }());

        ASSERT_NO_THROW([&]() {
            auto rd = alkaid::RecursiveDirectoryIterator("d2");
            while (rd) {
                if(!rd->st().ok()) {
                    throw std::runtime_error("Invalid directory entry");
                }
                rd++;
            }
        }());
        for (const auto &p: result) {
            os << p << ",";
        }
        ASSERT_TRUE(os.str() == "d2/b,d2/ds1,d2/ds2,");
    }
}