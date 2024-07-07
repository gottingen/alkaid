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

#include <gtest/gtest.h>
#include "helper.h"
#include <alkaid/files/filesystem.h>
#include <thread>

#if defined(WIN32) || defined(_WIN32)
#include <windows.h>
#else

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#endif

TEST(filesystem_error, filesystem_error) {
    std::error_code ec(1, std::system_category());
    alkaid::filesystem_error fse("None", std::error_code());
    fse = alkaid::filesystem_error("Some error", ec);
    ASSERT_TRUE(fse.code().value() == 1);
    ASSERT_TRUE(!std::string(fse.what()).empty());
    ASSERT_TRUE(fse.path1().empty());
    ASSERT_TRUE(fse.path2().empty());
    fse = alkaid::filesystem_error("Some error", alkaid::FilePath("foo/bar"), ec);
    ASSERT_TRUE(!std::string(fse.what()).empty());
    ASSERT_TRUE(fse.path1() == "foo/bar");
    ASSERT_TRUE(fse.path2().empty());
    fse = alkaid::filesystem_error("Some error", alkaid::FilePath("foo/bar"), alkaid::FilePath("some/other"), ec);
    ASSERT_TRUE(!std::string(fse.what()).empty());
    ASSERT_TRUE(fse.path1() == "foo/bar");
    ASSERT_TRUE(fse.path2() == "some/other");
}


static constexpr alkaid::Perms constExprOwnerAll() {
    return alkaid::Perms::owner_read | alkaid::Perms::owner_write | alkaid::Perms::owner_exec;
}

TEST(enum, Perms) {
    static_assert(constExprOwnerAll() == alkaid::Perms::owner_all, "constexpr didn't result in owner_all");
    ASSERT_TRUE((alkaid::Perms::owner_read | alkaid::Perms::owner_write | alkaid::Perms::owner_exec) ==
                alkaid::Perms::owner_all);
    ASSERT_TRUE((alkaid::Perms::group_read | alkaid::Perms::group_write | alkaid::Perms::group_exec) ==
                alkaid::Perms::group_all);
    ASSERT_TRUE((alkaid::Perms::others_read | alkaid::Perms::others_write | alkaid::Perms::others_exec) ==
                alkaid::Perms::others_all);
    ASSERT_TRUE(
            (alkaid::Perms::owner_all | alkaid::Perms::group_all | alkaid::Perms::others_all) == alkaid::Perms::all);
    ASSERT_TRUE((alkaid::Perms::all | alkaid::Perms::set_uid | alkaid::Perms::set_gid | alkaid::Perms::sticky_bit) ==
                alkaid::Perms::mask);
}

TEST(FileStatus, FileStatus) {
    {
        alkaid::FileStatus fs;
        ASSERT_TRUE(fs.type() == alkaid::FileType::none);
        ASSERT_TRUE(fs.permissions() == alkaid::Perms::unknown);
    }
    {
        alkaid::FileStatus fs{alkaid::FileType::regular};
        ASSERT_TRUE(fs.type() == alkaid::FileType::regular);
        ASSERT_TRUE(fs.permissions() == alkaid::Perms::unknown);
    }
    {
        alkaid::FileStatus fs{alkaid::FileType::directory,
                              alkaid::Perms::owner_read | alkaid::Perms::owner_write | alkaid::Perms::owner_exec};
        ASSERT_TRUE(fs.type() == alkaid::FileType::directory);
        ASSERT_TRUE(fs.permissions() == alkaid::Perms::owner_all);
        fs.type(alkaid::FileType::block);
        ASSERT_TRUE(fs.type() == alkaid::FileType::block);
        fs.type(alkaid::FileType::character);
        ASSERT_TRUE(fs.type() == alkaid::FileType::character);
        fs.type(alkaid::FileType::fifo);
        ASSERT_TRUE(fs.type() == alkaid::FileType::fifo);
        fs.type(alkaid::FileType::symlink);
        ASSERT_TRUE(fs.type() == alkaid::FileType::symlink);
        fs.type(alkaid::FileType::socket);
        ASSERT_TRUE(fs.type() == alkaid::FileType::socket);
        fs.permissions(fs.permissions() | alkaid::Perms::group_all | alkaid::Perms::others_all);
        ASSERT_TRUE(fs.permissions() == alkaid::Perms::all);
    }
    {
        alkaid::FileStatus fst(alkaid::FileType::regular);
        alkaid::FileStatus fs(std::move(fst));
        ASSERT_TRUE(fs.type() == alkaid::FileType::regular);
        ASSERT_TRUE(fs.permissions() == alkaid::Perms::unknown);
    }
#if !defined(USE_STD_FS) || defined(ALKAID_FILESYSTEM_RUNNING_CPP20)
    {
        alkaid::FileStatus fs1{alkaid::FileType::regular,
                               alkaid::Perms::owner_read | alkaid::Perms::owner_write | alkaid::Perms::owner_exec};
        alkaid::FileStatus fs2{alkaid::FileType::regular,
                               alkaid::Perms::owner_read | alkaid::Perms::owner_write | alkaid::Perms::owner_exec};
        alkaid::FileStatus fs3{alkaid::FileType::directory,
                               alkaid::Perms::owner_read | alkaid::Perms::owner_write | alkaid::Perms::owner_exec};
        alkaid::FileStatus fs4{alkaid::FileType::regular, alkaid::Perms::owner_read | alkaid::Perms::owner_write};
        ASSERT_TRUE(fs1 == fs2);
        ASSERT_FALSE(fs1 == fs3);
        ASSERT_FALSE(fs1 == fs4);
    }
#endif
}

TEST(fs, absolute) {
    ASSERT_TRUE(alkaid::absolute("").value() == alkaid::current_path().value() / "");
    ASSERT_TRUE(alkaid::absolute(alkaid::current_path().value()) == alkaid::current_path());
    ASSERT_TRUE(alkaid::absolute(".").value() == alkaid::current_path().value() / ".");
    ASSERT_TRUE((alkaid::absolute("..").value() == alkaid::current_path().value().parent_path() ||
                 alkaid::absolute("..").value() == alkaid::current_path().value() / ".."));
    ASSERT_TRUE(alkaid::absolute("foo").value() == alkaid::current_path().value() / "foo");
    std::error_code ec;
    ASSERT_TRUE(alkaid::absolute("", ec) == alkaid::current_path().value() / "");
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(alkaid::absolute("foo", ec) == alkaid::current_path().value() / "foo");
    ASSERT_TRUE(!ec);
}


TEST(fs, canonical) {
    ASSERT_FALSE(alkaid::canonical("").ok());
    {
        std::error_code ec;
        ASSERT_TRUE(alkaid::canonical("", ec) == "");
        ASSERT_TRUE(ec);
    }
    ASSERT_TRUE(alkaid::canonical(alkaid::current_path().value()).value() == alkaid::current_path().value());

    ASSERT_TRUE(alkaid::canonical(".").value() == alkaid::current_path().value());
    ASSERT_TRUE(alkaid::canonical("..").value() == alkaid::current_path().value().parent_path());
    ASSERT_TRUE(alkaid::canonical("/").value() == alkaid::current_path().value().root_path());
    ASSERT_FALSE(alkaid::canonical("foo").ok());
    {
        std::error_code ec;
        ASSERT_NO_THROW(alkaid::canonical("foo", ec));
        ASSERT_TRUE(ec);
    }
    {
        TemporaryDirectory t(TempOpt::change_path);
        auto dir = t.path() / "d0";
        ASSERT_TRUE(alkaid::create_directories(dir / "d1").ok());
        generateFile(dir / "f0");
        alkaid::FilePath rel(dir.filename());
        ASSERT_TRUE(alkaid::canonical(dir).value() == dir);
        ASSERT_TRUE(alkaid::canonical(rel).value() == dir);
        ASSERT_TRUE(alkaid::canonical(dir / "f0").value() == dir / "f0");
        ASSERT_TRUE(alkaid::canonical(rel / "f0").value() == dir / "f0");
        ASSERT_TRUE(alkaid::canonical(rel / "./f0").value() == dir / "f0");
        ASSERT_TRUE(alkaid::canonical(rel / "d1/../f0").value() == dir / "f0");
    }

    if (is_symlink_creation_supported()) {
        TemporaryDirectory t(TempOpt::change_path);
        ASSERT_TRUE(alkaid::create_directory(t.path() / "dir1").ok());
        generateFile(t.path() / "dir1/test1");
        ASSERT_TRUE(alkaid::create_directory(t.path() / "dir2").ok());
        ASSERT_TRUE(alkaid::create_directory_symlink(t.path() / "dir1", t.path() / "dir2/dirSym").ok());
        ASSERT_TRUE(alkaid::canonical(t.path() / "dir2/dirSym/test1").value() == t.path() / "dir1/test1");
    }
}


TEST(fs, copy) {
    {
        TemporaryDirectory t(TempOpt::change_path);
        std::error_code ec;
        ASSERT_TRUE(alkaid::create_directory("dir1").ok());
        generateFile("dir1/file1");
        generateFile("dir1/file2");
        ASSERT_TRUE(alkaid::create_directory("dir1/dir2").ok());
        generateFile("dir1/dir2/file3");
        ASSERT_TRUE(alkaid::copy("dir1", "dir3").ok());
        ASSERT_TRUE(alkaid::exists("dir3/file1").value());
        ASSERT_TRUE(alkaid::exists("dir3/file2").value());
        ASSERT_TRUE(!alkaid::exists("dir3/dir2").value());
        ASSERT_NO_THROW(alkaid::copy("dir1", "dir4", alkaid::CopyOptions::recursive, ec));
        ASSERT_TRUE(!ec);
        ASSERT_TRUE(alkaid::exists("dir4/file1").value());
        ASSERT_TRUE(alkaid::exists("dir4/file2").value());
        ASSERT_TRUE(alkaid::exists("dir4/dir2/file3").value());
        ASSERT_TRUE(alkaid::create_directory("dir5").ok());
        generateFile("dir5/file1");
        ASSERT_FALSE(alkaid::copy("dir1/file1", "dir5/file1").ok());
        ASSERT_TRUE(alkaid::copy("dir1/file1", "dir5/file1", alkaid::CopyOptions::skip_existing).ok());
    }
    if (is_symlink_creation_supported()) {
        TemporaryDirectory t(TempOpt::change_path);
        std::error_code ec;
        ASSERT_TRUE(alkaid::create_directory("dir1").ok());
        generateFile("dir1/file1");
        generateFile("dir1/file2");
        ASSERT_TRUE(alkaid::create_directory("dir1/dir2").ok());
        generateFile("dir1/dir2/file3");
#ifdef TEST_LWG_2682_BEHAVIOUR
        ASSERT_FALSE(alkaid::copy("dir1", "dir3",
                                  alkaid::CopyOptions::create_symlinks | alkaid::CopyOptions::recursive).ok());
#else
        auto rs = alkaid::copy("dir1", "dir3",
                               alkaid::CopyOptions::create_symlinks | alkaid::CopyOptions::recursive);
        ASSERT_TRUE(rs.ok())<<rs.message();
        ASSERT_TRUE(!ec);
        ASSERT_TRUE(alkaid::exists("dir3/file1").value());
        ASSERT_TRUE(alkaid::is_symlink("dir3/file1").value());
        ASSERT_TRUE(alkaid::exists("dir3/file2").value());
        ASSERT_TRUE(alkaid::is_symlink("dir3/file2").value());
        ASSERT_TRUE(alkaid::exists("dir3/dir2/file3").value());
        ASSERT_TRUE(alkaid::is_symlink("dir3/dir2/file3").value());
#endif
    }
#ifndef ALKAID_OS_WEB
    {
        TemporaryDirectory t(TempOpt::change_path);
        std::error_code ec;
        ASSERT_TRUE(alkaid::create_directory("dir1").ok());
        generateFile("dir1/file1");
        generateFile("dir1/file2");
        ASSERT_TRUE(alkaid::create_directory("dir1/dir2").ok());
        generateFile("dir1/dir2/file3");
        auto f1hl = alkaid::hard_link_count("dir1/file1").value();
        auto f2hl = alkaid::hard_link_count("dir1/file2").value();
        auto f3hl = alkaid::hard_link_count("dir1/dir2/file3").value();
        ASSERT_NO_THROW(
                alkaid::copy("dir1", "dir3", alkaid::CopyOptions::create_hard_links | alkaid::CopyOptions::recursive,
                             ec));
        ASSERT_TRUE(!ec);
        ASSERT_TRUE(alkaid::exists("dir3/file1").value());
        ASSERT_TRUE(alkaid::hard_link_count("dir1/file1").value() == f1hl + 1);
        ASSERT_TRUE(alkaid::exists("dir3/file2").value());
        ASSERT_TRUE(alkaid::hard_link_count("dir1/file2").value() == f2hl + 1);
        ASSERT_TRUE(alkaid::exists("dir3/dir2/file3").value());
        ASSERT_TRUE(alkaid::hard_link_count("dir1/dir2/file3").value() == f3hl + 1);
    }
#endif
}

TEST(fs, copy_file) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    generateFile("foo", 100);
    ASSERT_TRUE(!alkaid::exists("bar").value());
    ASSERT_TRUE(alkaid::copy_file("foo", "bar").value());
    ASSERT_TRUE(alkaid::exists("bar").value());
    ASSERT_TRUE(alkaid::file_size("foo") == alkaid::file_size("bar"));
    ASSERT_TRUE(alkaid::copy_file("foo", "bar2", ec));
    ASSERT_TRUE(!ec);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    generateFile("foo2", 200);
    ASSERT_TRUE(alkaid::copy_file("foo2", "bar", alkaid::CopyOptions::update_existing).value());
    ASSERT_TRUE(alkaid::file_size("bar").value() == 200);
    ASSERT_TRUE(!alkaid::copy_file("foo", "bar", alkaid::CopyOptions::update_existing).value());
    ASSERT_TRUE(alkaid::file_size("bar").value() == 200);
    ASSERT_TRUE(alkaid::copy_file("foo", "bar", alkaid::CopyOptions::overwrite_existing).value());
    ASSERT_TRUE(alkaid::file_size("bar").value() == 100);
    ASSERT_FALSE(alkaid::copy_file("foobar", "foobar2").ok());
    ASSERT_NO_THROW(alkaid::copy_file("foobar", "foobar2", ec));
    ASSERT_TRUE(ec);
    ASSERT_TRUE(!alkaid::exists("foobar").value());
    alkaid::FilePath file1("temp1.txt");
    alkaid::FilePath file2("temp2.txt");
    generateFile(file1, 200);
    generateFile(file2, 200);
    auto allWrite = alkaid::Perms::owner_write | alkaid::Perms::group_write | alkaid::Perms::others_write;
    ASSERT_TRUE(alkaid::permissions(file1, allWrite, alkaid::PermOptions::remove).ok());
    ASSERT_TRUE(
            (alkaid::status(file1).value().permissions() & alkaid::Perms::owner_write) != alkaid::Perms::owner_write);
    ASSERT_TRUE(alkaid::permissions(file2, allWrite, alkaid::PermOptions::add).ok());
    ASSERT_TRUE(
            (alkaid::status(file2).value().permissions() & alkaid::Perms::owner_write) == alkaid::Perms::owner_write);
    ASSERT_TRUE(alkaid::copy_file(file1, file2, alkaid::CopyOptions::overwrite_existing).ok());
    ASSERT_TRUE(
            (alkaid::status(file2).value().permissions() & alkaid::Perms::owner_write) != alkaid::Perms::owner_write);
    ASSERT_TRUE(alkaid::permissions(file1, allWrite, alkaid::PermOptions::add).ok());
    ASSERT_TRUE(alkaid::permissions(file2, allWrite, alkaid::PermOptions::add).ok());
}

TEST(fs, copy_symlink) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    generateFile("foo");
    ASSERT_TRUE(alkaid::create_directory("dir").ok());
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(alkaid::create_symlink("foo", "sfoo").ok());
        ASSERT_TRUE(alkaid::create_directory_symlink("dir", "sdir").ok());
        ASSERT_TRUE(alkaid::copy_symlink("sfoo", "sfooc").ok());
        ASSERT_TRUE(alkaid::exists("sfooc").value());
        ASSERT_NO_THROW(alkaid::copy_symlink("sfoo", "sfooc2", ec));
        ASSERT_TRUE(alkaid::exists("sfooc2").value());
        ASSERT_TRUE(!ec);
        ASSERT_TRUE(alkaid::copy_symlink("sdir", "sdirc").ok());
        ASSERT_TRUE(alkaid::exists("sdirc").value());
        ASSERT_NO_THROW(alkaid::copy_symlink("sdir", "sdirc2", ec));
        ASSERT_TRUE(alkaid::exists("sdirc2").value());
        ASSERT_TRUE(!ec);
    }
    ASSERT_FALSE(alkaid::copy_symlink("bar", "barc").ok());
    ASSERT_NO_THROW(alkaid::copy_symlink("bar", "barc", ec));
    ASSERT_TRUE(ec);
}

TEST(fs, create_directories) {
    TemporaryDirectory t;
    alkaid::FilePath p = t.path() / "testdir";
    alkaid::FilePath p2 = p / "nested";
    ASSERT_TRUE(!alkaid::exists(p).value());
    ASSERT_TRUE(!alkaid::exists(p2).value());
    ASSERT_TRUE(alkaid::create_directories(p2).value());
    ASSERT_TRUE(alkaid::is_directory(p).value());
    ASSERT_TRUE(alkaid::is_directory(p2).value());
    ASSERT_TRUE(!alkaid::create_directories(p2).value());
#ifdef TEST_LWG_2935_BEHAVIOUR
    INFO("This test expects LWG #2935 result conformance.");
        p = t.path() / "testfile";
        generateFile(p);
        ASSERT_TRUE(alkaid::is_regular_file(p));
        ASSERT_TRUE(!alkaid::is_directory(p));
        bool created = false;
        CHECK_NOTHROW((created = alkaid::create_directories(p)));
        ASSERT_TRUE(!created);
        ASSERT_TRUE(alkaid::is_regular_file(p));
        ASSERT_TRUE(!alkaid::is_directory(p));
        std::error_code ec;
        CHECK_NOTHROW((created = alkaid::create_directories(p, ec)));
        ASSERT_TRUE(!created);
        ASSERT_TRUE(!ec);
        ASSERT_TRUE(alkaid::is_regular_file(p));
        ASSERT_TRUE(!alkaid::is_directory(p));
        ASSERT_TRUE(!alkaid::create_directories(p, ec));
#else
    p = t.path() / "testfile";
    generateFile(p);
    ASSERT_TRUE(alkaid::is_regular_file(p).value());
    ASSERT_TRUE(!alkaid::is_directory(p).value());
    ASSERT_FALSE(alkaid::create_directories(p).ok());
    ASSERT_TRUE(alkaid::is_regular_file(p).value());
    ASSERT_TRUE(!alkaid::is_directory(p).value());
    std::error_code ec;
    ASSERT_NO_THROW(alkaid::create_directories(p, ec));
    ASSERT_TRUE(ec);
    ASSERT_TRUE(alkaid::is_regular_file(p).value());
    ASSERT_TRUE(!alkaid::is_directory(p).value());
    ASSERT_TRUE(!alkaid::create_directories(p, ec));
#endif
}


TEST(fs, create_directory) {
    TemporaryDirectory t;
    alkaid::FilePath p = t.path() / "testdir";
    ASSERT_TRUE(!alkaid::exists(p).value());
    ASSERT_TRUE(alkaid::create_directory(p).value());
    ASSERT_TRUE(alkaid::is_directory(p).value());
    ASSERT_TRUE(!alkaid::is_regular_file(p).value());
    ASSERT_TRUE(alkaid::create_directory(p / "nested", p).value());
    ASSERT_TRUE(alkaid::is_directory(p / "nested").value());
    ASSERT_TRUE(!alkaid::is_regular_file(p / "nested").value());
#ifdef TEST_LWG_2935_BEHAVIOUR
    INFO("This test expects LWG #2935 result conformance.");
        p = t.path() / "testfile";
        generateFile(p);
        ASSERT_TRUE(alkaid::is_regular_file(p));
        ASSERT_TRUE(!alkaid::is_directory(p));
        bool created = false;
        CHECK_NOTHROW((created = alkaid::create_directory(p)));
        ASSERT_TRUE(!created);
        ASSERT_TRUE(alkaid::is_regular_file(p));
        ASSERT_TRUE(!alkaid::is_directory(p));
        std::error_code ec;
        CHECK_NOTHROW((created = alkaid::create_directory(p, ec)));
        ASSERT_TRUE(!created);
        ASSERT_TRUE(!ec);
        ASSERT_TRUE(alkaid::is_regular_file(p));
        ASSERT_TRUE(!alkaid::is_directory(p));
        ASSERT_TRUE(!alkaid::create_directories(p, ec));
#else
    p = t.path() / "testfile";
    generateFile(p);
    ASSERT_TRUE(alkaid::is_regular_file(p).value());
    ASSERT_TRUE(!alkaid::is_directory(p).value());
    ASSERT_FALSE(alkaid::create_directory(p).ok());
    ASSERT_TRUE(alkaid::is_regular_file(p).ok());
    ASSERT_TRUE(!alkaid::is_directory(p).value());
    std::error_code ec;
    ASSERT_NO_THROW(alkaid::create_directory(p, ec));
    ASSERT_TRUE(ec);
    ASSERT_TRUE(alkaid::is_regular_file(p).value());
    ASSERT_TRUE(!alkaid::is_directory(p).value());
    ASSERT_TRUE(!alkaid::create_directory(p, ec));
#endif
}

TEST(fs, create_directory_symlink) {
    if (is_symlink_creation_supported()) {
        TemporaryDirectory t;
        ASSERT_TRUE(alkaid::create_directory(t.path() / "dir1").value());
        generateFile(t.path() / "dir1/test1");
        ASSERT_TRUE(alkaid::create_directory(t.path() / "dir2").value());
        auto rs = alkaid::create_directory_symlink(t.path() / "dir1", t.path() / "dir2/dirSym");
        ASSERT_TRUE(rs.ok()) << rs.to_string();
        ASSERT_TRUE(alkaid::exists(t.path() / "dir2/dirSym").value());
        ASSERT_TRUE(alkaid::is_symlink(t.path() / "dir2/dirSym").value());
        ASSERT_TRUE(alkaid::exists(t.path() / "dir2/dirSym/test1").value());
        ASSERT_TRUE(alkaid::is_regular_file(t.path() / "dir2/dirSym/test1").value());
        ASSERT_FALSE(alkaid::create_directory_symlink(t.path() / "dir1", t.path() / "dir2/dirSym").ok());
        std::error_code ec;
        ASSERT_NO_THROW(alkaid::create_directory_symlink(t.path() / "dir1", t.path() / "dir2/dirSym", ec));
        ASSERT_TRUE(ec);
    }
}

TEST(fs, create_hard_link) {
#ifndef ALKAID_OS_WEB
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    generateFile("foo", 1234);
    ASSERT_TRUE(alkaid::create_hard_link("foo", "bar").ok());
    ASSERT_TRUE(alkaid::exists("bar").value());
    ASSERT_TRUE(!alkaid::is_symlink("bar").value());
    ASSERT_NO_THROW(alkaid::create_hard_link("foo", "bar2", ec));
    ASSERT_TRUE(alkaid::exists("bar2").value());
    ASSERT_TRUE(!alkaid::is_symlink("bar2").value());
    ASSERT_TRUE(!ec);
    ASSERT_FALSE(alkaid::create_hard_link("nofoo", "bar").ok());
    ASSERT_NO_THROW(alkaid::create_hard_link("nofoo", "bar", ec));
    ASSERT_TRUE(ec);
#endif
}

TEST(fs, create_symlink) {
    if (is_symlink_creation_supported()) {
        TemporaryDirectory t;
        ASSERT_TRUE(alkaid::create_directory(t.path() / "dir1").value());
        generateFile(t.path() / "dir1/test1");
        ASSERT_TRUE(alkaid::create_directory(t.path() / "dir2").value());
        ASSERT_TRUE(alkaid::create_symlink(t.path() / "dir1/test1", t.path() / "dir2/fileSym").ok());
        ASSERT_TRUE(alkaid::exists(t.path() / "dir2/fileSym").value());
        ASSERT_TRUE(alkaid::is_symlink(t.path() / "dir2/fileSym").value()) << std::to_string(
                            static_cast<int>(alkaid::status(t.path() / "dir2/fileSym").value().type()));
        ASSERT_TRUE(alkaid::exists(t.path() / "dir2/fileSym").value());
        ASSERT_TRUE(alkaid::is_regular_file(t.path() / "dir2/fileSym").value());
        ASSERT_FALSE(alkaid::create_symlink(t.path() / "dir1", t.path() / "dir2/fileSym").ok());
        std::error_code ec;
        ASSERT_NO_THROW(alkaid::create_symlink(t.path() / "dir1", t.path() / "dir2/fileSym", ec));
        ASSERT_TRUE(ec);
    }
}

TEST(fs, current_path) {
    TemporaryDirectory t;
    std::error_code ec;
    alkaid::FilePath p1 = alkaid::current_path().value();
    ASSERT_TRUE(alkaid::current_path(t.path()).ok());
    ASSERT_TRUE(p1 != alkaid::current_path().value());
    ASSERT_NO_THROW(alkaid::current_path(p1, ec));
    ASSERT_TRUE(!ec);
    ASSERT_FALSE(alkaid::current_path(t.path() / "foo").ok());
    ASSERT_TRUE(p1 == alkaid::current_path().value());
    ASSERT_NO_THROW(alkaid::current_path(t.path() / "foo", ec));
    ASSERT_TRUE(ec);
}


TEST(fs, equivalent) {
    TemporaryDirectory t(TempOpt::change_path);
    generateFile("foo", 1234);
    ASSERT_TRUE(alkaid::equivalent(t.path() / "foo", "foo").value());
    if (is_symlink_creation_supported()) {
        std::error_code ec(42, std::system_category());
        ASSERT_TRUE(alkaid::create_symlink("foo", "foo2").ok());
        ASSERT_TRUE(alkaid::equivalent("foo", "foo2").value());
        ASSERT_TRUE(alkaid::equivalent("foo", "foo2", ec));
        ASSERT_TRUE(!ec);
    }
#ifdef TEST_LWG_2937_BEHAVIOUR
    std::error_code ec;
    bool result = false;
    ASSERT_FALSE(alkaid::equivalent("foo", "foo3").ok());
    ASSERT_NO_THROW(result = alkaid::equivalent("foo", "foo3", ec));
    ASSERT_TRUE(!result);
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_FALSE(alkaid::equivalent("foo3", "foo").ok());
    ASSERT_NO_THROW(result = alkaid::equivalent("foo3", "foo", ec));
    ASSERT_TRUE(!result);
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_FALSE(alkaid::equivalent("foo3", "foo4").ok());
    ASSERT_NO_THROW(result = alkaid::equivalent("foo3", "foo4", ec));
    ASSERT_TRUE(!result);
    ASSERT_TRUE(ec);
#else
    INFO("This test expects conformance predating LWG #2937 result.");
        std::error_code ec;
        bool result = false;
        ASSERT_TRUE_NOTHROW(result = alkaid::equivalent("foo", "foo3"));
        ASSERT_TRUE(!result);
        CHECK_NOTHROW(result = alkaid::equivalent("foo", "foo3", ec));
        ASSERT_TRUE(!result);
        ASSERT_TRUE(!ec);
        ec.clear();
        CHECK_NOTHROW(result = alkaid::equivalent("foo3", "foo"));
        ASSERT_TRUE(!result);
        CHECK_NOTHROW(result = alkaid::equivalent("foo3", "foo", ec));
        ASSERT_TRUE(!result);
        ASSERT_TRUE(!ec);
        ec.clear();
        CHECK_THROWS_AS(result = alkaid::equivalent("foo4", "foo3"), alkaid::filesystem_error);
        ASSERT_TRUE(!result);
        CHECK_NOTHROW(result = alkaid::equivalent("foo4", "foo3", ec));
        ASSERT_TRUE(!result);
        ASSERT_TRUE(ec);
#endif
}

TEST(fs, exists) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    ASSERT_TRUE(!alkaid::exists("").value());
    ASSERT_TRUE(!alkaid::exists("foo").value());
    ASSERT_TRUE(!alkaid::exists("foo", ec));
    ASSERT_TRUE(!ec);
    ec = std::error_code(42, std::system_category());
    ASSERT_TRUE(!alkaid::exists("foo", ec));
#if defined(__cpp_lib_char8_t) && !defined(ALKAID_FILESYSTEM_ENFORCE_CPP17_API)
    ASSERT_TRUE(!alkaid::exists(u8"foo"));
#endif
    ASSERT_TRUE(!ec);
    ec.clear();
    ASSERT_TRUE(alkaid::exists(t.path()).value());
    ASSERT_TRUE(alkaid::exists(t.path(), ec));
    ASSERT_TRUE(!ec);
    ec = std::error_code(42, std::system_category());
    ASSERT_TRUE(alkaid::exists(t.path(), ec));
    ASSERT_TRUE(!ec);
#if defined(ALKAID_OS_WINDOWS) && !defined(ALKAID_FILESYSTEM_FWD)
    if (::GetFileAttributesW(L"C:\\fs-test") != INVALID_FILE_ATTRIBUTES) {
            ASSERT_TRUE(alkaid::exists("C:\\fs-test"));
        }
#endif
}

TEST(fs, file_size) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    generateFile("foo", 0);
    generateFile("bar", 1234);
    ASSERT_TRUE(alkaid::file_size("foo").value() == 0);
    ec = std::error_code(42, std::system_category());
    ASSERT_TRUE(alkaid::file_size("foo", ec) == 0);
    ASSERT_TRUE(!ec);
    ec.clear();
    ASSERT_TRUE(alkaid::file_size("bar").value() == 1234);
    ec = std::error_code(42, std::system_category());
    ASSERT_TRUE(alkaid::file_size("bar", ec) == 1234);
    ASSERT_TRUE(!ec);
    ec.clear();
    ASSERT_FALSE(alkaid::file_size("foobar").ok());
    ASSERT_TRUE(alkaid::file_size("foobar", ec) == static_cast<uintmax_t>(-1));
    ASSERT_TRUE(ec);
    ec.clear();
}


#ifndef ALKAID_OS_WINDOWS

static uintmax_t getHardlinkCount(const alkaid::FilePath &p) {
    struct stat st = {};
    auto rc = ::lstat(p.c_str(), &st);
    return rc == 0 ? st.st_nlink : ~0u;
}

#endif

TEST(fs, hard_link_count) {
#ifndef ALKAID_OS_WEB
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
#ifdef ALKAID_OS_WINDOWS
    // windows doesn't implement "."/".." as hardlinks, so it
        // starts with 1 and subdirectories don't change the count
        ASSERT_TRUE(alkaid::hard_link_count(t.path()) == 1);
        alkaid::create_directory("dir");
        ASSERT_TRUE(alkaid::hard_link_count(t.path()) == 1);
#else
// unix/bsd/linux typically implements "."/".." as hardlinks
// so an empty dir has 2 (from parent and the ".") and
// adding a subdirectory adds one due to its ".."
    ASSERT_TRUE(alkaid::hard_link_count(t.path()).value() == getHardlinkCount(t.path()));
    ASSERT_TRUE(alkaid::create_directory("dir").value());
    ASSERT_TRUE(alkaid::hard_link_count(t.path()).value() == getHardlinkCount(t.path()));
#endif
    generateFile("foo");
    ASSERT_TRUE(alkaid::hard_link_count(t.path() / "foo").value() == 1);
    ec = std::error_code(42, std::system_category());
    ASSERT_TRUE(alkaid::hard_link_count(t.path() / "foo", ec) == 1);
    ASSERT_TRUE(!ec);
    ASSERT_FALSE(alkaid::hard_link_count(t.path() / "bar").ok());
    ASSERT_NO_THROW(alkaid::hard_link_count(t.path() / "bar", ec));
    ASSERT_TRUE(ec);
    ec.clear();
#else
    WARN("Test for unsupportet features are disabled on JS/Wasm target.");
#endif
}


TEST(fs, temp_dir_path) {
    std::error_code ec;
    ASSERT_TRUE(alkaid::exists(alkaid::temp_directory_path().value()).ok());
    ASSERT_TRUE(alkaid::exists(alkaid::temp_directory_path(ec)).ok());
    ASSERT_TRUE(!alkaid::temp_directory_path().value().empty());
    ASSERT_TRUE(!ec);
}

TEST(fs, weakly_canonical) {
    ASSERT_TRUE(alkaid::weakly_canonical("").value() == ".");
    if (alkaid::weakly_canonical("").value() == ".") {
        ASSERT_TRUE(alkaid::weakly_canonical("foo/bar").value() == "foo/bar");
        ASSERT_TRUE(alkaid::weakly_canonical("foo/./bar").value() == "foo/bar");
        ASSERT_TRUE(alkaid::weakly_canonical("foo/../bar").value() == "bar");
    } else {
        ASSERT_TRUE(alkaid::weakly_canonical("foo/bar").value() == alkaid::current_path().value() / "foo/bar");
        ASSERT_TRUE(alkaid::weakly_canonical("foo/./bar").value() == alkaid::current_path().value() / "foo/bar");
        ASSERT_TRUE(alkaid::weakly_canonical("foo/../bar").value() == alkaid::current_path().value() / "bar");
    }

    {
        TemporaryDirectory t(TempOpt::change_path);
        auto dir = t.path() / "d0";
        ASSERT_TRUE(alkaid::create_directories(dir / "d1").ok());
        generateFile(dir / "f0");
        alkaid::FilePath rel(dir.filename());
        ASSERT_TRUE(alkaid::weakly_canonical(dir).value() == dir);
        ASSERT_TRUE(alkaid::weakly_canonical(rel).value() == dir);
        ASSERT_TRUE(alkaid::weakly_canonical(dir / "f0").value() == dir / "f0");
        ASSERT_TRUE(alkaid::weakly_canonical(dir / "f0/").value() == dir / "f0/");
        ASSERT_TRUE(alkaid::weakly_canonical(dir / "f1").value() == dir / "f1");
        ASSERT_TRUE(alkaid::weakly_canonical(rel / "f0").value() == dir / "f0");
        ASSERT_TRUE(alkaid::weakly_canonical(rel / "f0/").value() == dir / "f0/");
        ASSERT_TRUE(alkaid::weakly_canonical(rel / "f1").value() == dir / "f1");
        ASSERT_TRUE(alkaid::weakly_canonical(rel / "./f0").value() == dir / "f0");
        ASSERT_TRUE(alkaid::weakly_canonical(rel / "./f1").value() == dir / "f1");
        ASSERT_TRUE(alkaid::weakly_canonical(rel / "d1/../f0").value() == dir / "f0");
        ASSERT_TRUE(alkaid::weakly_canonical(rel / "d1/../f1").value() == dir / "f1");
        ASSERT_TRUE(alkaid::weakly_canonical(rel / "d1/../f1/../f2").value() == dir / "f2");
    }
}


TEST(fs, status_known) {
    ASSERT_TRUE(!alkaid::status_known(alkaid::FileStatus()));
    ASSERT_TRUE(alkaid::status_known(alkaid::FileStatus(alkaid::FileType::not_found)));
    ASSERT_TRUE(alkaid::status_known(alkaid::FileStatus(alkaid::FileType::regular)));
    ASSERT_TRUE(alkaid::status_known(alkaid::FileStatus(alkaid::FileType::directory)));
    ASSERT_TRUE(alkaid::status_known(alkaid::FileStatus(alkaid::FileType::symlink)));
    ASSERT_TRUE(alkaid::status_known(alkaid::FileStatus(alkaid::FileType::character)));
    ASSERT_TRUE(alkaid::status_known(alkaid::FileStatus(alkaid::FileType::fifo)));
    ASSERT_TRUE(alkaid::status_known(alkaid::FileStatus(alkaid::FileType::socket)));
    ASSERT_TRUE(alkaid::status_known(alkaid::FileStatus(alkaid::FileType::unknown)));
}

TEST(fs, symlink_status) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    alkaid::FileStatus fs;
    ASSERT_NO_THROW(fs = alkaid::symlink_status("foo").value());
    ASSERT_TRUE(fs.type() == alkaid::FileType::not_found);
    ASSERT_TRUE(fs.permissions() == alkaid::Perms::unknown);
    ASSERT_NO_THROW(fs = alkaid::symlink_status("bar", ec));
    ASSERT_TRUE(fs.type() == alkaid::FileType::not_found);
    ASSERT_TRUE(fs.permissions() == alkaid::Perms::unknown);
    ASSERT_TRUE(ec);
    ec.clear();
    fs = alkaid::symlink_status(t.path()).value();
    ASSERT_TRUE(fs.type() == alkaid::FileType::directory);
    ASSERT_TRUE((fs.permissions() & (alkaid::Perms::owner_read | alkaid::Perms::owner_write)) ==
                (alkaid::Perms::owner_read | alkaid::Perms::owner_write));
    generateFile("foobar");
    fs = alkaid::symlink_status(t.path() / "foobar").value();
    ASSERT_TRUE(fs.type() == alkaid::FileType::regular);
    ASSERT_TRUE((fs.permissions() & (alkaid::Perms::owner_read | alkaid::Perms::owner_write)) ==
                (alkaid::Perms::owner_read | alkaid::Perms::owner_write));
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(alkaid::create_symlink(t.path() / "foobar", t.path() / "barfoo").ok());
        fs = alkaid::symlink_status(t.path() / "barfoo").value();
        ASSERT_TRUE(fs.type() == alkaid::FileType::symlink);
    }
}


TEST(fs, status) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    alkaid::FileStatus fs;
    ASSERT_NO_THROW(fs = alkaid::status("foo").value());
    ASSERT_TRUE(fs.type() == alkaid::FileType::not_found);
    ASSERT_TRUE(fs.permissions() == alkaid::Perms::unknown);
    ASSERT_NO_THROW(fs = alkaid::status("bar", ec));
    ASSERT_TRUE(fs.type() == alkaid::FileType::not_found);
    ASSERT_TRUE(fs.permissions() == alkaid::Perms::unknown);
    ASSERT_TRUE(ec);
    ec.clear();
    fs = alkaid::status(t.path()).value();
    ASSERT_TRUE(fs.type() == alkaid::FileType::directory);
    ASSERT_TRUE((fs.permissions() & (alkaid::Perms::owner_read | alkaid::Perms::owner_write)) ==
                (alkaid::Perms::owner_read | alkaid::Perms::owner_write));
    generateFile("foobar");
    fs = alkaid::status(t.path() / "foobar").value();
    ASSERT_TRUE(fs.type() == alkaid::FileType::regular);
    ASSERT_TRUE((fs.permissions() & (alkaid::Perms::owner_read | alkaid::Perms::owner_write)) ==
                (alkaid::Perms::owner_read | alkaid::Perms::owner_write));
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(alkaid::create_symlink(t.path() / "foobar", t.path() / "barfoo").ok());
        fs = alkaid::status(t.path() / "barfoo").value();
        ASSERT_TRUE(fs.type() == alkaid::FileType::regular);
        ASSERT_TRUE((fs.permissions() & (alkaid::Perms::owner_read | alkaid::Perms::owner_write)) ==
                    (alkaid::Perms::owner_read | alkaid::Perms::owner_write));
    }
}


TEST(fs, space) {
    {
        alkaid::SpaceInfo si;
        ASSERT_NO_THROW(si = alkaid::space(alkaid::current_path().value()).value());
        ASSERT_TRUE(si.capacity > 1024 * 1024);
        ASSERT_TRUE(si.capacity > si.free);
        ASSERT_TRUE(si.free >= si.available);
    }
    {
        std::error_code ec;
        alkaid::SpaceInfo si;
        ASSERT_NO_THROW(si = alkaid::space(alkaid::current_path().value(), ec));
        ASSERT_TRUE(si.capacity > 1024 * 1024);
        ASSERT_TRUE(si.capacity > si.free);
        ASSERT_TRUE(si.free >= si.available);
        ASSERT_TRUE(!ec);
    }
#ifndef ALKAID_OS_WEB // statvfs under emscripten always returns a result, so this tests would fail
    {
        std::error_code ec;
        alkaid::SpaceInfo si;
        ASSERT_NO_THROW(si = alkaid::space("foobar42", ec));
        ASSERT_TRUE(si.capacity == static_cast<uintmax_t>(-1));
        ASSERT_TRUE(si.free == static_cast<uintmax_t>(-1));
        ASSERT_TRUE(si.available == static_cast<uintmax_t>(-1));
        ASSERT_TRUE(ec);
    }
    ASSERT_FALSE(alkaid::space("foobar42").ok());
#endif
}


TEST(fs, remove_all) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    generateFile("foo");
    ASSERT_TRUE(alkaid::remove_all("foo", ec) == 1);
    ASSERT_TRUE(!ec);
    ec.clear();
    ASSERT_TRUE(alkaid::DirectoryIterator(t.path()) == alkaid::DirectoryIterator());
    ASSERT_TRUE(alkaid::create_directories("dir1/dir1a").ok());
    ASSERT_TRUE(alkaid::create_directories("dir1/dir1b").ok());
    generateFile("dir1/dir1a/f1");
    generateFile("dir1/dir1b/f2");
    ASSERT_NO_THROW(alkaid::remove_all("dir1/non-existing", ec));
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(alkaid::remove_all("dir1/non-existing", ec) == 0);
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(alkaid::create_directory_symlink("dir1", "dir1link").ok());
        ASSERT_TRUE(alkaid::remove_all("dir1link").value() == 1);
    }
    ASSERT_TRUE(alkaid::remove_all("dir1").value() == 5);
    ASSERT_TRUE(alkaid::DirectoryIterator(t.path()) == alkaid::DirectoryIterator());
}

TEST(fs, rename) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    generateFile("foo", 123);
    ASSERT_TRUE(alkaid::create_directory("dir1").ok());
    ASSERT_TRUE(alkaid::rename("foo", "bar").ok());
    ASSERT_TRUE(!alkaid::exists("foo").value());
    ASSERT_TRUE(alkaid::exists("bar").value());
    ASSERT_TRUE(alkaid::rename("dir1", "dir2").ok());
    ASSERT_TRUE(alkaid::exists("dir2").value());
    generateFile("foo2", 42);
    ASSERT_TRUE(alkaid::rename("bar", "foo2").ok());
    ASSERT_TRUE(alkaid::exists("foo2").value());
    ASSERT_TRUE(alkaid::file_size("foo2").value() == 123u);
    ASSERT_TRUE(!alkaid::exists("bar").value());
    ASSERT_NO_THROW(alkaid::rename("foo2", "foo", ec));
    ASSERT_TRUE(!ec);
    ASSERT_FALSE(alkaid::rename("foobar", "barfoo").ok());
    ASSERT_NO_THROW(alkaid::rename("foobar", "barfoo", ec));
    ASSERT_TRUE(ec);
    ASSERT_TRUE(!alkaid::exists("barfoo").value());
}

TEST(fs, resize_file) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    generateFile("foo", 1024);
    ASSERT_TRUE(alkaid::file_size("foo").value() == 1024);
    ASSERT_TRUE(alkaid::resize_file("foo", 2048).ok());
    ASSERT_TRUE(alkaid::file_size("foo").value() == 2048);
    ASSERT_NO_THROW(alkaid::resize_file("foo", 1000, ec));
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(alkaid::file_size("foo").value() == 1000);
    ASSERT_FALSE(alkaid::resize_file("bar", 2048).ok());
    ASSERT_TRUE(!alkaid::exists("bar").value());
    ASSERT_NO_THROW(alkaid::resize_file("bar", 4096, ec));
    ASSERT_TRUE(ec);
    ASSERT_TRUE(!alkaid::exists("bar").value());
}


#ifndef ALKAID_OS_WEB

static turbo::Time timeFromString(const std::string &str) {
    struct ::tm tm;
    ::memset(&tm, 0, sizeof(::tm));
    std::istringstream is(str);
    is >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (is.fail()) {
        throw std::exception();
    }
    return turbo::Time::from_time_t(std::mktime(&tm));
}

#endif

TEST(fs, last_write_time) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    turbo::Time ft;
    generateFile("foo");
    auto now = turbo::Time::current_time();
    ASSERT_TRUE(turbo::Duration::to_seconds(alkaid::last_write_time(t.path()).value() - now) < 3);
    ASSERT_TRUE(turbo::Duration::to_seconds(alkaid::last_write_time("foo").value() - now) < 3);
    ASSERT_FALSE(alkaid::last_write_time("bar").ok());
    ASSERT_NO_THROW(ft = alkaid::last_write_time("bar", ec));
    ASSERT_TRUE(ft == turbo::Time());
    ASSERT_TRUE(ec);
    ec.clear();
    if (is_symlink_creation_supported()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        ASSERT_TRUE(alkaid::create_symlink("foo", "foo2").ok());
        ft = alkaid::last_write_time("foo").value();
// checks that the time of the symlink is fetched
        ASSERT_TRUE(ft == alkaid::last_write_time("foo2").value());
    }
#ifndef ALKAID_OS_WEB
    auto nt = timeFromString("2015-10-21T04:30:00");
    ASSERT_TRUE(alkaid::last_write_time(t.path() / "foo", nt).ok());
    ASSERT_TRUE(turbo::Duration::to_seconds(alkaid::last_write_time("foo").value() - nt) < 1);
    nt = timeFromString("2015-10-21T04:29:00");
    ASSERT_NO_THROW(alkaid::last_write_time("foo", nt, ec));
    ASSERT_TRUE(turbo::Duration::to_seconds(alkaid::last_write_time("foo").value() - nt) < 1);
    ASSERT_TRUE(!ec);
    ASSERT_FALSE(alkaid::last_write_time("bar", nt).ok());
    ASSERT_NO_THROW(alkaid::last_write_time("bar", nt, ec));
    ASSERT_TRUE(ec);
#endif
}

TEST(fs, permissions) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    generateFile("foo", 512);
    auto allWrite = alkaid::Perms::owner_write | alkaid::Perms::group_write | alkaid::Perms::others_write;
    ASSERT_TRUE(alkaid::permissions("foo", allWrite, alkaid::PermOptions::remove).ok());
    ASSERT_TRUE(
            (alkaid::status("foo").value().permissions() & alkaid::Perms::owner_write) != alkaid::Perms::owner_write);
#if !defined(ALKAID_OS_WINDOWS)
    if (geteuid() != 0)
#endif
    {
        ASSERT_FALSE(alkaid::resize_file("foo", 1024).ok());
        ASSERT_TRUE(alkaid::file_size("foo").value() == 512);
    }
    ASSERT_TRUE(alkaid::permissions("foo", alkaid::Perms::owner_write, alkaid::PermOptions::add).ok());
    ASSERT_TRUE(
            (alkaid::status("foo").value().permissions() & alkaid::Perms::owner_write) == alkaid::Perms::owner_write);
    ASSERT_TRUE(alkaid::resize_file("foo", 2048).ok());
    ASSERT_TRUE(alkaid::file_size("foo").value() == 2048);
    ASSERT_FALSE(alkaid::permissions("bar", alkaid::Perms::owner_write, alkaid::PermOptions::add).ok());
    ASSERT_NO_THROW(alkaid::permissions("bar", alkaid::Perms::owner_write, alkaid::PermOptions::add, ec));
    ASSERT_TRUE(ec);
    ASSERT_FALSE(alkaid::permissions("bar", alkaid::Perms::owner_write, static_cast<alkaid::PermOptions>(0)).ok());
}

TEST(fs, proximate) {
    std::error_code ec;
    ASSERT_TRUE(alkaid::proximate("/a/d", "/a/b/c").value() == "../../d");
    ASSERT_TRUE(alkaid::proximate("/a/d", "/a/b/c", ec) == "../../d");
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(alkaid::proximate("/a/b/c", "/a/d").value() == "../b/c");
    ASSERT_TRUE(alkaid::proximate("/a/b/c", "/a/d", ec) == "../b/c");
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(alkaid::proximate("a/b/c", "a").value() == "b/c");
    ASSERT_TRUE(alkaid::proximate("a/b/c", "a", ec) == "b/c");
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(alkaid::proximate("a/b/c", "a/b/c/x/y").value() == "../..");
    ASSERT_TRUE(alkaid::proximate("a/b/c", "a/b/c/x/y", ec) == "../..");
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(alkaid::proximate("a/b/c", "a/b/c").value() == ".");
    ASSERT_TRUE(alkaid::proximate("a/b/c", "a/b/c", ec) == ".");
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(alkaid::proximate("a/b", "c/d").value() == "../../a/b");
    ASSERT_TRUE(alkaid::proximate("a/b", "c/d", ec) == "../../a/b");
    ASSERT_TRUE(!ec);
#ifndef ALKAID_OS_WINDOWS
    if (has_host_root_name_support()) {
        ASSERT_TRUE(alkaid::proximate("//host1/a/d", "//host2/a/b/c").value() == "//host1/a/d");
        ASSERT_TRUE(alkaid::proximate("//host1/a/d", "//host2/a/b/c", ec) == "//host1/a/d");
        ASSERT_TRUE(!ec);
    }
#endif
}

TEST(fs, read_symlink) {
    if (is_symlink_creation_supported()) {
        TemporaryDirectory t(TempOpt::change_path);
        std::error_code ec;
        generateFile("foo");
        ASSERT_TRUE(alkaid::create_symlink(t.path() / "foo", "bar").ok());
        ASSERT_TRUE(alkaid::read_symlink("bar").value() == t.path() / "foo");
        ASSERT_TRUE(alkaid::read_symlink("bar", ec) == t.path() / "foo");
        ASSERT_TRUE(!ec);
        ASSERT_FALSE(alkaid::read_symlink("foobar").ok());
        ASSERT_TRUE(alkaid::read_symlink("foobar", ec) == alkaid::FilePath());
        ASSERT_TRUE(ec);
    }
}

TEST(fs, relative) {
    ASSERT_TRUE(alkaid::relative("/a/d", "/a/b/c").value() == "../../d");
    ASSERT_TRUE(alkaid::relative("/a/b/c", "/a/d").value() == "../b/c");
    ASSERT_TRUE(alkaid::relative("a/b/c", "a").value() == "b/c");
    ASSERT_TRUE(alkaid::relative("a/b/c", "a/b/c/x/y").value() == "../..");
    ASSERT_TRUE(alkaid::relative("a/b/c", "a/b/c").value() == ".");
    ASSERT_TRUE(alkaid::relative("a/b", "c/d").value() == "../../a/b");
    std::error_code ec;
    ASSERT_TRUE(alkaid::relative(alkaid::current_path().value() / "foo", ec) == "foo");
    ASSERT_TRUE(!ec);
}

TEST(fs, remove) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    generateFile("foo");
    ASSERT_TRUE(alkaid::remove("foo").value());
    ASSERT_TRUE(!alkaid::exists("foo").value());
    ASSERT_TRUE(!alkaid::remove("foo").value());
    generateFile("foo");
    ASSERT_TRUE(alkaid::remove("foo", ec));
    ASSERT_TRUE(!alkaid::exists("foo").value());
    if (is_symlink_creation_supported()) {
        generateFile("foo");
        ASSERT_TRUE(alkaid::create_symlink("foo", "bar").ok());
        ASSERT_TRUE(alkaid::exists(alkaid::symlink_status("bar").value()));
        ASSERT_TRUE(alkaid::remove("bar", ec));
        ASSERT_TRUE(alkaid::exists("foo").value());
        ASSERT_TRUE(!alkaid::exists(alkaid::symlink_status("bar").value()));
    }
    ASSERT_TRUE(!alkaid::remove("bar").value());
    ASSERT_TRUE(!alkaid::remove("bar", ec));
    ASSERT_TRUE(!ec);
}


class FileTypeMixFixture : public testing::Test {
public:
    FileTypeMixFixture() {
    }

    void SetUp() override {
        _t = TemporaryDirectory(TempOpt::change_path);
        _hasFifo = false;
        _hasSocket = false;
        LOG(INFO)<< "Temporary directory: " << _t.path();
        generateFile(_t.path()/"regular");

        auto rs = alkaid::create_directory(_t.path()/"directory");
        ASSERT_TRUE(rs.ok())<< rs.status().message();
        if (is_symlink_creation_supported()) {
            auto rs = alkaid::create_symlink(_t.path()/"regular", _t.path()/"file_symlink");
            ASSERT_TRUE(rs.ok())<< rs.message();
            ASSERT_TRUE(alkaid::create_directory_symlink(_t.path()/"directory", _t.path()/"dir_symlink").ok());
        }
#if !defined(ALKAID_OS_WINDOWS) && !defined(ALKAID_OS_WEB)
        ASSERT_TRUE(::mkfifo((temp_path()/"fifo").c_str(), 0644) == 0);
        _hasFifo = true;
        struct ::sockaddr_un addr;
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, (temp_path()/"socket").c_str(), sizeof(addr.sun_path));
        int fd = socket(PF_UNIX, SOCK_STREAM, 0);
        (void) bind(fd, (struct sockaddr *) &addr, sizeof addr);
        _hasSocket = true;
#endif
    }

    void TearDown() override {

    }

    ~FileTypeMixFixture() {}

    bool has_fifo() const { return _hasFifo; }

    bool has_socket() const { return _hasSocket; }

    alkaid::FilePath block_path() const {
        std::error_code ec;
        if (alkaid::exists("/dev/sda", ec)) {
            return "/dev/sda";
        } else if (alkaid::exists("/dev/disk0", ec)) {
            return "/dev/disk0";
        }
        return alkaid::FilePath();
    }

    alkaid::FilePath character_path() const {
#ifndef ALKAID_OS_SOLARIS
        std::error_code ec;
        if (alkaid::exists("/dev/null", ec)) {
            return "/dev/null";
        } else if (alkaid::exists("NUL", ec)) {
            return "NUL";
        }
#endif
        return alkaid::FilePath();
    }

    alkaid::FilePath temp_path() const { return _t.path(); }

private:
    TemporaryDirectory _t;
    bool _hasFifo;
    bool _hasSocket;
};
/*
TEST_F(FileTypeMixFixture, is_block_file) {
    std::error_code ec;
    ASSERT_TRUE(!alkaid::is_block_file("directory").value());
    ASSERT_TRUE(!alkaid::is_block_file("regular").value());
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(!alkaid::is_block_file("dir_symlink").value());
        ASSERT_TRUE(!alkaid::is_block_file("file_symlink").value());
    }
    ASSERT_TRUE((has_fifo() ? !alkaid::is_block_file("fifo").value() : true));
    ASSERT_TRUE((has_socket() ? !alkaid::is_block_file("socket").value() : true));
    ASSERT_TRUE((block_path().empty() ? true : alkaid::is_block_file(block_path())).value());
    ASSERT_TRUE((character_path().empty() ? true : !alkaid::is_block_file(character_path()).value()));
    ASSERT_TRUE(alkaid::is_block_file("notfound").ok());
    ASSERT_NO_THROW(alkaid::is_block_file("notfound", ec));
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_TRUE(!alkaid::is_block_file(alkaid::FileStatus(alkaid::FileType::none)));
    ASSERT_TRUE(!alkaid::is_block_file(alkaid::FileStatus(alkaid::FileType::not_found)));
    ASSERT_TRUE(!alkaid::is_block_file(alkaid::FileStatus(alkaid::FileType::regular)));
    ASSERT_TRUE(!alkaid::is_block_file(alkaid::FileStatus(alkaid::FileType::directory)));
    ASSERT_TRUE(!alkaid::is_block_file(alkaid::FileStatus(alkaid::FileType::symlink)));
    ASSERT_TRUE(alkaid::is_block_file(alkaid::FileStatus(alkaid::FileType::block)));
    ASSERT_TRUE(!alkaid::is_block_file(alkaid::FileStatus(alkaid::FileType::character)));
    ASSERT_TRUE(!alkaid::is_block_file(alkaid::FileStatus(alkaid::FileType::fifo)));
    ASSERT_TRUE(!alkaid::is_block_file(alkaid::FileStatus(alkaid::FileType::socket)));
    ASSERT_TRUE(!alkaid::is_block_file(alkaid::FileStatus(alkaid::FileType::unknown)));
}

TEST_F(FileTypeMixFixture, is_character_file) {
    std::error_code ec;
    ASSERT_TRUE(!alkaid::is_character_file(temp_path()/"directory").value());
    ASSERT_TRUE(!alkaid::is_character_file(temp_path()/"regular").value());
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(!alkaid::is_character_file(temp_path()/"dir_symlink").value());
        ASSERT_TRUE(!alkaid::is_character_file(temp_path()/"file_symlink").value());
    }
    ASSERT_TRUE((has_fifo() ? !alkaid::is_character_file(temp_path()/"fifo").value() : true));
    ASSERT_TRUE((has_socket() ? !alkaid::is_character_file(temp_path()/"socket").value() : true));
    ASSERT_TRUE((block_path().empty() ? true : !alkaid::is_character_file(block_path()).value()));
    ASSERT_TRUE((character_path().empty() ? true : alkaid::is_character_file(character_path()).value()));
    ASSERT_TRUE(alkaid::is_character_file(temp_path()/"notfound").ok());
    ASSERT_NO_THROW(alkaid::is_character_file(temp_path()/"notfound", ec));
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_TRUE(!alkaid::is_character_file(alkaid::FileStatus(alkaid::FileType::none)));
    ASSERT_TRUE(!alkaid::is_character_file(alkaid::FileStatus(alkaid::FileType::not_found)));
    ASSERT_TRUE(!alkaid::is_character_file(alkaid::FileStatus(alkaid::FileType::regular)));
    ASSERT_TRUE(!alkaid::is_character_file(alkaid::FileStatus(alkaid::FileType::directory)));
    ASSERT_TRUE(!alkaid::is_character_file(alkaid::FileStatus(alkaid::FileType::symlink)));
    ASSERT_TRUE(!alkaid::is_character_file(alkaid::FileStatus(alkaid::FileType::block)));
    ASSERT_TRUE(alkaid::is_character_file(alkaid::FileStatus(alkaid::FileType::character)));
    ASSERT_TRUE(!alkaid::is_character_file(alkaid::FileStatus(alkaid::FileType::fifo)));
    ASSERT_TRUE(!alkaid::is_character_file(alkaid::FileStatus(alkaid::FileType::socket)));
    ASSERT_TRUE(!alkaid::is_character_file(alkaid::FileStatus(alkaid::FileType::unknown)));
}

TEST_F(FileTypeMixFixture, is_directory) {
    std::error_code ec;
    ASSERT_TRUE(alkaid::is_directory("directory").value());
    ASSERT_TRUE(!alkaid::is_directory("regular").value());
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(alkaid::is_directory("dir_symlink").value());
        ASSERT_TRUE(!alkaid::is_directory("file_symlink").value());
    }
    ASSERT_TRUE((has_fifo() ? !alkaid::is_directory("fifo").value() : true));
    ASSERT_TRUE((has_socket() ? !alkaid::is_directory("socket").value() : true));
    ASSERT_TRUE((block_path().empty() ? true : !alkaid::is_directory(block_path()).value()));
    ASSERT_TRUE((character_path().empty() ? true : !alkaid::is_directory(character_path()).value()));
    ASSERT_TRUE(alkaid::is_directory("notfound").ok());
    ASSERT_NO_THROW(alkaid::is_directory("notfound", ec));
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_TRUE(!alkaid::is_directory(alkaid::FileStatus(alkaid::FileType::none)));
    ASSERT_TRUE(!alkaid::is_directory(alkaid::FileStatus(alkaid::FileType::not_found)));
    ASSERT_TRUE(!alkaid::is_directory(alkaid::FileStatus(alkaid::FileType::regular)));
    ASSERT_TRUE(alkaid::is_directory(alkaid::FileStatus(alkaid::FileType::directory)));
    ASSERT_TRUE(!alkaid::is_directory(alkaid::FileStatus(alkaid::FileType::symlink)));
    ASSERT_TRUE(!alkaid::is_directory(alkaid::FileStatus(alkaid::FileType::block)));
    ASSERT_TRUE(!alkaid::is_directory(alkaid::FileStatus(alkaid::FileType::character)));
    ASSERT_TRUE(!alkaid::is_directory(alkaid::FileStatus(alkaid::FileType::fifo)));
    ASSERT_TRUE(!alkaid::is_directory(alkaid::FileStatus(alkaid::FileType::socket)));
    ASSERT_TRUE(!alkaid::is_directory(alkaid::FileStatus(alkaid::FileType::unknown)));
}

TEST(fs, is_empty) {
    TemporaryDirectory t(TempOpt::change_path);
    std::error_code ec;
    ASSERT_TRUE(alkaid::is_empty(t.path()).value());
    ASSERT_TRUE(alkaid::is_empty(t.path(), ec));
    ASSERT_TRUE(!ec);
    generateFile("foo", 0);
    generateFile("bar", 1234);
    ASSERT_TRUE(alkaid::is_empty("foo").value());
    ASSERT_TRUE(alkaid::is_empty("foo", ec));
    ASSERT_TRUE(!ec);
    ASSERT_TRUE(!alkaid::is_empty("bar").value());
    ASSERT_TRUE(!alkaid::is_empty("bar", ec));
    ASSERT_TRUE(!ec);
    ASSERT_FALSE(alkaid::is_empty("foobar").ok());
    bool result = false;
    ASSERT_NO_THROW(result = alkaid::is_empty("foobar", ec));
    ASSERT_TRUE(!result);
    ASSERT_TRUE(ec);
}

TEST_F(FileTypeMixFixture, is_fifo) {
    std::error_code ec;
    ASSERT_TRUE(!alkaid::is_fifo("directory").value());
    ASSERT_TRUE(!alkaid::is_fifo("regular").value());
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(!alkaid::is_fifo("dir_symlink").value());
        ASSERT_TRUE(!alkaid::is_fifo("file_symlink").value());
    }
    ASSERT_TRUE((has_fifo() ? alkaid::is_fifo("fifo").value() : true));
    ASSERT_TRUE((has_socket() ? !alkaid::is_fifo("socket").value() : true));
    ASSERT_TRUE((block_path().empty() ? true : !alkaid::is_fifo(block_path()).value()));
    ASSERT_TRUE((character_path().empty() ? true : !alkaid::is_fifo(character_path()).value()));
    ASSERT_TRUE(alkaid::is_fifo("notfound").ok());
    ASSERT_NO_THROW(alkaid::is_fifo("notfound", ec));
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_TRUE(!alkaid::is_fifo(alkaid::FileStatus(alkaid::FileType::none)));
    ASSERT_TRUE(!alkaid::is_fifo(alkaid::FileStatus(alkaid::FileType::not_found)));
    ASSERT_TRUE(!alkaid::is_fifo(alkaid::FileStatus(alkaid::FileType::regular)));
    ASSERT_TRUE(!alkaid::is_fifo(alkaid::FileStatus(alkaid::FileType::directory)));
    ASSERT_TRUE(!alkaid::is_fifo(alkaid::FileStatus(alkaid::FileType::symlink)));
    ASSERT_TRUE(!alkaid::is_fifo(alkaid::FileStatus(alkaid::FileType::block)));
    ASSERT_TRUE(!alkaid::is_fifo(alkaid::FileStatus(alkaid::FileType::character)));
    ASSERT_TRUE(alkaid::is_fifo(alkaid::FileStatus(alkaid::FileType::fifo)));
    ASSERT_TRUE(!alkaid::is_fifo(alkaid::FileStatus(alkaid::FileType::socket)));
    ASSERT_TRUE(!alkaid::is_fifo(alkaid::FileStatus(alkaid::FileType::unknown)));
}

TEST_F(FileTypeMixFixture, is_other) {
    std::error_code ec;
    ASSERT_TRUE(!alkaid::is_other("directory").value());
    ASSERT_TRUE(!alkaid::is_other("regular").value());
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(!alkaid::is_other("dir_symlink").value());
        ASSERT_TRUE(!alkaid::is_other("file_symlink").value());
    }
    ASSERT_TRUE((has_fifo() ? alkaid::is_other("fifo").value() : true));
    ASSERT_TRUE((has_socket() ? alkaid::is_other("socket").value() : true));
    ASSERT_TRUE((block_path().empty() ? true : alkaid::is_other(block_path()).value()));
    ASSERT_TRUE((character_path().empty() ? true : alkaid::is_other(character_path()).value()));
    ASSERT_TRUE(alkaid::is_other("notfound").ok());
    ASSERT_NO_THROW(alkaid::is_other("notfound", ec));
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_TRUE(!alkaid::is_other(alkaid::FileStatus(alkaid::FileType::none)));
    ASSERT_TRUE(!alkaid::is_other(alkaid::FileStatus(alkaid::FileType::not_found)));
    ASSERT_TRUE(!alkaid::is_other(alkaid::FileStatus(alkaid::FileType::regular)));
    ASSERT_TRUE(!alkaid::is_other(alkaid::FileStatus(alkaid::FileType::directory)));
    ASSERT_TRUE(!alkaid::is_other(alkaid::FileStatus(alkaid::FileType::symlink)));
    ASSERT_TRUE(alkaid::is_other(alkaid::FileStatus(alkaid::FileType::block)));
    ASSERT_TRUE(alkaid::is_other(alkaid::FileStatus(alkaid::FileType::character)));
    ASSERT_TRUE(alkaid::is_other(alkaid::FileStatus(alkaid::FileType::fifo)));
    ASSERT_TRUE(alkaid::is_other(alkaid::FileStatus(alkaid::FileType::socket)));
    ASSERT_TRUE(alkaid::is_other(alkaid::FileStatus(alkaid::FileType::unknown)));
}

TEST_F(FileTypeMixFixture, is_regular_file) {
    std::error_code ec;
    ASSERT_TRUE(!alkaid::is_regular_file("directory").value());
    ASSERT_TRUE(alkaid::is_regular_file("regular").value());
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(!alkaid::is_regular_file("dir_symlink").value());
        ASSERT_TRUE(alkaid::is_regular_file("file_symlink").value());
    }
    ASSERT_TRUE((has_fifo() ? !alkaid::is_regular_file("fifo").value() : true));
    ASSERT_TRUE((has_socket() ? !alkaid::is_regular_file("socket").value() : true));
    ASSERT_TRUE((block_path().empty() ? true : !alkaid::is_regular_file(block_path()).value()));
    ASSERT_TRUE((character_path().empty() ? true : !alkaid::is_regular_file(character_path()).value()));
    ASSERT_TRUE(alkaid::is_regular_file("notfound").ok());
    ASSERT_NO_THROW(alkaid::is_regular_file("notfound", ec));
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_TRUE(!alkaid::is_regular_file(alkaid::FileStatus(alkaid::FileType::none)));
    ASSERT_TRUE(!alkaid::is_regular_file(alkaid::FileStatus(alkaid::FileType::not_found)));
    ASSERT_TRUE(alkaid::is_regular_file(alkaid::FileStatus(alkaid::FileType::regular)));
    ASSERT_TRUE(!alkaid::is_regular_file(alkaid::FileStatus(alkaid::FileType::directory)));
    ASSERT_TRUE(!alkaid::is_regular_file(alkaid::FileStatus(alkaid::FileType::symlink)));
    ASSERT_TRUE(!alkaid::is_regular_file(alkaid::FileStatus(alkaid::FileType::block)));
    ASSERT_TRUE(!alkaid::is_regular_file(alkaid::FileStatus(alkaid::FileType::character)));
    ASSERT_TRUE(!alkaid::is_regular_file(alkaid::FileStatus(alkaid::FileType::fifo)));
    ASSERT_TRUE(!alkaid::is_regular_file(alkaid::FileStatus(alkaid::FileType::socket)));
    ASSERT_TRUE(!alkaid::is_regular_file(alkaid::FileStatus(alkaid::FileType::unknown)));
}

TEST_F(FileTypeMixFixture, is_socket) {
    std::error_code ec;
    ASSERT_TRUE(!alkaid::is_socket("directory").value());
    ASSERT_TRUE(!alkaid::is_socket("regular").value());
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(!alkaid::is_socket("dir_symlink").value());
        ASSERT_TRUE(!alkaid::is_socket("file_symlink").value());
    }
    ASSERT_TRUE((has_fifo() ? !alkaid::is_socket("fifo").value() : true));
    ASSERT_TRUE((has_socket() ? alkaid::is_socket("socket").value() : true));
    ASSERT_TRUE((block_path().empty() ? true : !alkaid::is_socket(block_path()).value()));
    ASSERT_TRUE((character_path().empty() ? true : !alkaid::is_socket(character_path()).value()));
    ASSERT_TRUE(alkaid::is_socket("notfound").ok());
    ASSERT_NO_THROW(alkaid::is_socket("notfound", ec));
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_TRUE(!alkaid::is_socket(alkaid::FileStatus(alkaid::FileType::none)));
    ASSERT_TRUE(!alkaid::is_socket(alkaid::FileStatus(alkaid::FileType::not_found)));
    ASSERT_TRUE(!alkaid::is_socket(alkaid::FileStatus(alkaid::FileType::regular)));
    ASSERT_TRUE(!alkaid::is_socket(alkaid::FileStatus(alkaid::FileType::directory)));
    ASSERT_TRUE(!alkaid::is_socket(alkaid::FileStatus(alkaid::FileType::symlink)));
    ASSERT_TRUE(!alkaid::is_socket(alkaid::FileStatus(alkaid::FileType::block)));
    ASSERT_TRUE(!alkaid::is_socket(alkaid::FileStatus(alkaid::FileType::character)));
    ASSERT_TRUE(!alkaid::is_socket(alkaid::FileStatus(alkaid::FileType::fifo)));
    ASSERT_TRUE(alkaid::is_socket(alkaid::FileStatus(alkaid::FileType::socket)));
    ASSERT_TRUE(!alkaid::is_socket(alkaid::FileStatus(alkaid::FileType::unknown)));
}

TEST_F(FileTypeMixFixture, is_symlink) {
    std::error_code ec;
    ASSERT_TRUE(!alkaid::is_symlink("directory").value());
    ASSERT_TRUE(!alkaid::is_symlink("regular").value());
    if (is_symlink_creation_supported()) {
        ASSERT_TRUE(alkaid::is_symlink("dir_symlink").value());
        ASSERT_TRUE(alkaid::is_symlink("file_symlink").value());
    }
    ASSERT_TRUE((has_fifo() ? !alkaid::is_symlink("fifo").value() : true));
    ASSERT_TRUE((has_socket() ? !alkaid::is_symlink("socket").value() : true));
    ASSERT_TRUE((block_path().empty() ? true : !alkaid::is_symlink(block_path()).value()));
    ASSERT_TRUE((character_path().empty() ? true : !alkaid::is_symlink(character_path()).value()));
    ASSERT_TRUE(alkaid::is_symlink("notfound").ok());
    ASSERT_NO_THROW(alkaid::is_symlink("notfound", ec));
    ASSERT_TRUE(ec);
    ec.clear();
    ASSERT_TRUE(!alkaid::is_symlink(alkaid::FileStatus(alkaid::FileType::none)));
    ASSERT_TRUE(!alkaid::is_symlink(alkaid::FileStatus(alkaid::FileType::not_found)));
    ASSERT_TRUE(!alkaid::is_symlink(alkaid::FileStatus(alkaid::FileType::regular)));
    ASSERT_TRUE(!alkaid::is_symlink(alkaid::FileStatus(alkaid::FileType::directory)));
    ASSERT_TRUE(alkaid::is_symlink(alkaid::FileStatus(alkaid::FileType::symlink)));
    ASSERT_TRUE(!alkaid::is_symlink(alkaid::FileStatus(alkaid::FileType::block)));
    ASSERT_TRUE(!alkaid::is_symlink(alkaid::FileStatus(alkaid::FileType::character)));
    ASSERT_TRUE(!alkaid::is_symlink(alkaid::FileStatus(alkaid::FileType::fifo)));
    ASSERT_TRUE(!alkaid::is_symlink(alkaid::FileStatus(alkaid::FileType::socket)));
    ASSERT_TRUE(!alkaid::is_symlink(alkaid::FileStatus(alkaid::FileType::unknown)));
}

TEST(FileSystemTest, SequentialReadMMapFile) {
    auto fs = alkaid::Filesystem::localfs();
    EXPECT_TRUE(fs != nullptr);
    auto file_rs = fs->create_sequential_write_file();
    EXPECT_TRUE(file_rs.ok());
    auto file = file_rs.value();
    auto status = file->open("test.txt", alkaid::lfs::kDefaultTruncateWriteOption, {});
    EXPECT_TRUE(status.ok()) << status.message();
    auto size = file->size();
    EXPECT_TRUE(size.ok());
    EXPECT_EQ(size.value(), 0);
    char buff[1024];
    auto read = file->append(buff, 1024);
    EXPECT_TRUE(read.ok());
    status = file->close();
    EXPECT_TRUE(status.ok());
    auto read_file_rs = fs->create_sequential_read_file();
    EXPECT_TRUE(read_file_rs.ok());
    auto read_file = read_file_rs.value();
    status = read_file->open("test.txt", std::any(), {});
    EXPECT_TRUE(status.ok());
    size = read_file->size();
    EXPECT_TRUE(size.ok());
    EXPECT_EQ(size.value(), 1024);
    char read_buff[1024];
    auto read_size = read_file->read(read_buff, 1024);
    EXPECT_TRUE(read_size.ok());
    EXPECT_EQ(read_size.value(), 1024);
    EXPECT_EQ(memcmp(buff, read_buff, 1024), 0);
}*/