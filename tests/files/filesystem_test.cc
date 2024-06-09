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

#include <alkaid/files/filesystem.h>

TEST(FileSystemTest, SequentialReadMMapFile) {
    auto fs = alkaid::Filesystem::localfs();
    EXPECT_TRUE(fs != nullptr);
    auto file_rs = fs->create_sequential_write_file();
    EXPECT_TRUE(file_rs.ok());
    auto file = file_rs.value();
    auto status = file->open("test.txt",  alkaid::lfs::kDefaultTruncateWriteOption, {});
    EXPECT_TRUE(status.ok())<<status.message();
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
}