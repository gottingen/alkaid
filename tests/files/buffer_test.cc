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
#include <alkaid/files/buffered_reader.h>
#include <alkaid/files/buffered_writer.h>
#include <alkaid/files/sequential_read_file.h>
#include <alkaid/files/sequential_write_file.h>


TEST(BufferedTest, host_endian) {
    auto file = std::make_shared<alkaid::SequentialWriteFile>();
    auto rs = file->open("test.txt", alkaid::kDefaultTruncateWriteOption, alkaid::FileEventListener{});
    ASSERT_TRUE(rs.ok());
    auto writer = alkaid::BufferedWriter<false>(file, 1024);
    std::string content = "hello world";
    rs = writer.write(content);
    ASSERT_TRUE(rs.ok());
    rs = writer.write_int32(123);
    ASSERT_TRUE(rs.ok());
    rs = writer.write_int64(123456);
    ASSERT_TRUE(rs.ok());
    rs = writer.write_uint32(123);
    ASSERT_TRUE(rs.ok());
    rs = writer.flush();
    ASSERT_TRUE(rs.ok());
    rs = writer.finalize();
    ASSERT_TRUE(rs.ok())<<rs.message();
    rs = file->close();
    ASSERT_TRUE(rs.ok());

    auto read_file = std::make_shared<alkaid::SequentialReadFile>();
    rs = read_file->open("test.txt", std::any{}, alkaid::FileEventListener{});
    ASSERT_TRUE(rs.ok());
    auto reader = alkaid::BufferedReader<false>(read_file, 1024);
    std::string result;
    auto rss = reader.read(content.size(), &result);
    ASSERT_TRUE(rss.ok());
    ASSERT_EQ(content, result);
    int32_t in32;
    rss = reader.read_int32();
    ASSERT_TRUE(rss.ok());
    in32 = rss.value();
    ASSERT_EQ(123, in32);
    int64_t in64;
    rss = reader.read_int64();
    ASSERT_TRUE(rss.ok());
    in64 = rss.value();
    ASSERT_EQ(123456, in64);
    uint32_t uin32;
    rss = reader.read_uint32();
    ASSERT_TRUE(rss.ok());
    uin32 = rss.value();
    ASSERT_EQ(123, uin32);
    rs = read_file->close();
    ASSERT_TRUE(rs.ok());
    ASSERT_TRUE(alkaid::remove("test.txt").ok());
}


TEST(BufferedTest, big_endian) {
    auto file = std::make_shared<alkaid::SequentialWriteFile>();
    auto rs = file->open("test1.txt", alkaid::kDefaultTruncateWriteOption, alkaid::FileEventListener{});
    ASSERT_TRUE(rs.ok());
    auto writer = alkaid::BufferedWriter<true>(file, 1024);
    std::string content = "hello world";
    rs = writer.write(content);
    ASSERT_TRUE(rs.ok());
    rs = writer.write_int32(123);
    ASSERT_TRUE(rs.ok());
    rs = writer.write_int64(123456);
    ASSERT_TRUE(rs.ok());
    rs = writer.write_uint32(123);
    ASSERT_TRUE(rs.ok());
    rs = writer.finalize();
    ASSERT_TRUE(rs.ok())<<rs.message();
    rs = file->close();
    ASSERT_TRUE(rs.ok());

    auto read_file = std::make_shared<alkaid::SequentialReadFile>();
    rs = read_file->open("test1.txt", std::any{}, alkaid::FileEventListener{});
    ASSERT_TRUE(rs.ok());
    auto reader = alkaid::BufferedReader<true>(read_file, 1024);
    std::string result;
    auto rss = reader.read(content.size(), &result);
    ASSERT_TRUE(rss.ok());
    ASSERT_EQ(content, result);
    int32_t in32;
    rss = reader.read_int32();
    ASSERT_TRUE(rss.ok());
    in32 = rss.value();
    ASSERT_EQ(123, in32);
    int64_t in64;
    rss = reader.read_int64();
    ASSERT_TRUE(rss.ok());
    in64 = rss.value();
    ASSERT_EQ(123456, in64);
    uint32_t uin32;
    rss = reader.read_uint32();
    ASSERT_TRUE(rss.ok());
    uin32 = rss.value();
    ASSERT_EQ(123, uin32);
    rs = read_file->close();
    ASSERT_TRUE(rs.ok());
    ASSERT_TRUE(alkaid::remove("test1.txt").ok());

}

TEST(BufferedTest, max_endian) {
    auto file = std::make_shared<alkaid::SequentialWriteFile>();
    auto rs = file->open("test.txt", alkaid::kDefaultTruncateWriteOption, alkaid::FileEventListener{});
    ASSERT_TRUE(rs.ok());
    auto writer = alkaid::BufferedWriter<false>(file, 1024);
    std::string content = "hello world";
    rs = writer.write(content);
    ASSERT_TRUE(rs.ok());
    rs = writer.write_int32(123);
    ASSERT_TRUE(rs.ok());
    rs = writer.write_int64(123456);
    ASSERT_TRUE(rs.ok());
    rs = writer.write_uint32(123);
    ASSERT_TRUE(rs.ok());
    rs = writer.finalize();
    ASSERT_TRUE(rs.ok());
    rs = file->close();
    ASSERT_TRUE(rs.ok());

    auto read_file = std::make_shared<alkaid::SequentialReadFile>();
    rs = read_file->open("test.txt", std::any{}, alkaid::FileEventListener{});
    ASSERT_TRUE(rs.ok());
    auto reader = alkaid::BufferedReader<true>(read_file, 1024);
    std::string result;
    auto rss = reader.read(content.size(), &result);
    ASSERT_TRUE(rss.ok());
    ASSERT_EQ(content, result);
    int32_t in32;
    rss = reader.read_int32();
    ASSERT_TRUE(rss.ok());
    in32 = rss.value();
    ASSERT_NE(123, in32);
    int64_t in64;
    rss = reader.read_int64();
    ASSERT_TRUE(rss.ok());
    in64 = rss.value();
    ASSERT_NE(123456, in64);
    uint32_t uin32;
    rss = reader.read_uint32();
    ASSERT_TRUE(rss.ok());
    uin32 = rss.value();
    ASSERT_NE(123, uin32);
    rs = read_file->close();
    ASSERT_TRUE(rs.ok());
    ASSERT_TRUE(alkaid::remove("test.txt").ok());

}