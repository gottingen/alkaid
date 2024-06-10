// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <vector>
#include <random>
#include <gtest/gtest.h>

#include <turbo/utility/status.h>
#include <alkaid/compress/compression.h>


#define ALKAID_ASSIGN_OR_RAISE_NAME(x, y) TURBO_CONCAT(x, y)

#define ASSIGN_OR_HANDLE_ERROR_IMPL(handle_error, status_name, lhs, rexpr) \
  auto&& status_name = (rexpr);                                            \
  handle_error(status_name.status().ok())<<status_name.status().message();                                      \
  lhs = std::move(status_name).value();

#define RESULT_OK_AND_ASSIGN(lhs, rexpr) \
  ASSIGN_OR_HANDLE_ERROR_IMPL(           \
      ASSERT_TRUE, ALKAID_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);

#define ASSERT_OK_AND_EQ(expected, expr)        \
  do {                                          \
    RESULT_OK_AND_ASSIGN(auto _actual, (expr)); \
    ASSERT_EQ(expected, _actual);               \
  } while (0)

namespace alkaid {
    void random_bytes(int64_t n, uint32_t seed, uint8_t *out) {
        std::mt19937 gen(seed);
        std::uniform_int_distribution<uint32_t> d(0, std::numeric_limits<uint8_t>::max());
        std::generate(out, out + n, [&d, &gen] { return static_cast<uint8_t>(d(gen)); });
    }

    std::vector<uint8_t> MakeRandomData(int data_size) {
        std::vector<uint8_t> data(data_size);
        random_bytes(data_size, 1234, data.data());
        return data;
    }

    std::vector<uint8_t> MakeCompressibleData(int data_size) {
        std::string base_data =
                "alkaid is a cross-language development platform for in-memory data";
        int nrepeats = static_cast<int>(1 + data_size / base_data.size());

        std::vector<uint8_t> data(base_data.size() * nrepeats);
        for (int i = 0; i < nrepeats; ++i) {
            std::memcpy(data.data() + i * base_data.size(), base_data.data(), base_data.size());
        }
        data.resize(data_size);
        return data;
    }

// Check roundtrip of one-shot compression and decompression functions.
    void CheckCodecRoundtrip(std::unique_ptr<Codec> &c1, std::unique_ptr<Codec> &c2,
                             const std::vector<uint8_t> &data, bool check_reverse = true) {
        int max_compressed_len =
                static_cast<int>(c1->MaxCompressedLen(data.size(), data.data()));
        std::vector<uint8_t> compressed(max_compressed_len);
        std::vector<uint8_t> decompressed(data.size());

        // compress with c1
        int64_t actual_size;
        RESULT_OK_AND_ASSIGN(actual_size, c1->Compress(data.size(), data.data(),
                                                       max_compressed_len, compressed.data()));
        compressed.resize(actual_size);

        // decompress with c2
        int64_t actual_decompressed_size;
        RESULT_OK_AND_ASSIGN(actual_decompressed_size,
                             c2->Decompress(compressed.size(), compressed.data(),
                                            decompressed.size(), decompressed.data()));

        ASSERT_EQ(data, decompressed);
        ASSERT_EQ(data.size(), actual_decompressed_size);

        if (check_reverse) {
            // compress with c2
            ASSERT_EQ(max_compressed_len,
                      static_cast<int>(c2->MaxCompressedLen(data.size(), data.data())));
            // Resize to prevent ASAN from detecting container overflow.
            compressed.resize(max_compressed_len);

            int64_t actual_size2;
            RESULT_OK_AND_ASSIGN(
                    actual_size2,
                    c2->Compress(data.size(), data.data(), max_compressed_len, compressed.data()));
            ASSERT_EQ(actual_size2, actual_size);
            compressed.resize(actual_size2);

            // decompress with c1
            int64_t actual_decompressed_size2;
            RESULT_OK_AND_ASSIGN(actual_decompressed_size2,
                                 c1->Decompress(compressed.size(), compressed.data(),
                                                decompressed.size(), decompressed.data()));

            ASSERT_EQ(data, decompressed);
            ASSERT_EQ(data.size(), actual_decompressed_size2);
        }
    }

// Check the streaming compressor against one-shot decompression

    void CheckStreamingCompressor(Codec *codec, const std::vector<uint8_t> &data) {
        std::shared_ptr<Compressor> compressor;
        RESULT_OK_AND_ASSIGN(compressor, codec->MakeCompressor());

        std::vector<uint8_t> compressed;
        int64_t compressed_size = 0;
        const uint8_t *input = data.data();
        int64_t remaining = data.size();

        compressed.resize(10);
        bool do_flush = false;

        while (remaining > 0) {
            // Feed a small amount each time
            int64_t input_len = std::min(remaining, static_cast<int64_t>(1111));
            int64_t output_len = compressed.size() - compressed_size;
            uint8_t *output = compressed.data() + compressed_size;
            RESULT_OK_AND_ASSIGN(auto result,
                                 compressor->Compress(input_len, input, output_len, output));
            ASSERT_LE(result.bytes_read, input_len);
            ASSERT_LE(result.bytes_written, output_len);
            compressed_size += result.bytes_written;
            input += result.bytes_read;
            remaining -= result.bytes_read;
            if (result.bytes_read == 0) {
                compressed.resize(compressed.capacity() * 2);
            }
            // Once every two iterations, do a flush
            if (do_flush) {
                Compressor::FlushResult result;
                do {
                    output_len = compressed.size() - compressed_size;
                    output = compressed.data() + compressed_size;
                    RESULT_OK_AND_ASSIGN(result, compressor->Flush(output_len, output));
                    ASSERT_LE(result.bytes_written, output_len);
                    compressed_size += result.bytes_written;
                    if (result.should_retry) {
                        compressed.resize(compressed.capacity() * 2);
                    }
                } while (result.should_retry);
            }
            do_flush = !do_flush;
        }

        // End the compressed stream
        Compressor::EndResult result;
        do {
            int64_t output_len = compressed.size() - compressed_size;
            uint8_t *output = compressed.data() + compressed_size;
            RESULT_OK_AND_ASSIGN(result, compressor->End(output_len, output));
            ASSERT_LE(result.bytes_written, output_len);
            compressed_size += result.bytes_written;
            if (result.should_retry) {
                compressed.resize(compressed.capacity() * 2);
            }
        } while (result.should_retry);

        // Check decompressing the compressed data
        std::vector<uint8_t> decompressed(data.size());
        ASSERT_TRUE(codec->Decompress(compressed_size, compressed.data(), decompressed.size(),
                                      decompressed.data()).ok());

        ASSERT_EQ(data, decompressed);
    }

// Check the streaming decompressor against one-shot compression

    void CheckStreamingDecompressor(Codec *codec, const std::vector<uint8_t> &data) {
        // Create compressed data
        int64_t max_compressed_len = codec->MaxCompressedLen(data.size(), data.data());
        std::vector<uint8_t> compressed(max_compressed_len);
        int64_t compressed_size;
        RESULT_OK_AND_ASSIGN(
                compressed_size,
                codec->Compress(data.size(), data.data(), max_compressed_len, compressed.data()));
        compressed.resize(compressed_size);

        // Run streaming decompression
        std::shared_ptr<Decompressor> decompressor;
        RESULT_OK_AND_ASSIGN(decompressor, codec->MakeDecompressor());

        std::vector<uint8_t> decompressed;
        int64_t decompressed_size = 0;
        const uint8_t *input = compressed.data();
        int64_t remaining = compressed.size();

        decompressed.resize(10);
        while (!decompressor->IsFinished()) {
            // Feed a small amount each time
            int64_t input_len = std::min(remaining, static_cast<int64_t>(23));
            int64_t output_len = decompressed.size() - decompressed_size;
            uint8_t *output = decompressed.data() + decompressed_size;
            RESULT_OK_AND_ASSIGN(auto result,
                                 decompressor->Decompress(input_len, input, output_len, output));
            ASSERT_LE(result.bytes_read, input_len);
            ASSERT_LE(result.bytes_written, output_len);
            ASSERT_TRUE(result.need_more_output || result.bytes_written > 0 ||
                        result.bytes_read > 0)
                                        << "Decompression not progressing anymore";
            if (result.need_more_output) {
                decompressed.resize(decompressed.capacity() * 2);
            }
            decompressed_size += result.bytes_written;
            input += result.bytes_read;
            remaining -= result.bytes_read;
        }
        ASSERT_TRUE(decompressor->IsFinished());
        ASSERT_EQ(remaining, 0);

        // Check the decompressed data
        decompressed.resize(decompressed_size);
        ASSERT_EQ(data.size(), decompressed_size);
        ASSERT_EQ(data, decompressed);
    }

// Check the streaming compressor and decompressor together

    void CheckStreamingRoundtrip(std::shared_ptr<Compressor> compressor,
                                 std::shared_ptr<Decompressor> decompressor,
                                 const std::vector<uint8_t> &data) {
        std::default_random_engine engine(42);
        std::uniform_int_distribution<int> buf_size_distribution(10, 40);

        auto make_buf_size = [&]() -> int64_t { return buf_size_distribution(engine); };

        // Compress...

        std::vector<uint8_t> compressed(1);
        int64_t compressed_size = 0;
        {
            const uint8_t *input = data.data();
            int64_t remaining = data.size();

            while (remaining > 0) {
                // Feed a varying amount each time
                int64_t input_len = std::min(remaining, make_buf_size());
                int64_t output_len = compressed.size() - compressed_size;
                uint8_t *output = compressed.data() + compressed_size;
                RESULT_OK_AND_ASSIGN(auto result,
                                     compressor->Compress(input_len, input, output_len, output));
                ASSERT_LE(result.bytes_read, input_len);
                ASSERT_LE(result.bytes_written, output_len);
                compressed_size += result.bytes_written;
                input += result.bytes_read;
                remaining -= result.bytes_read;
                if (result.bytes_read == 0) {
                    compressed.resize(compressed.capacity() * 2);
                }
            }
            // End the compressed stream
            Compressor::EndResult result;
            do {
                int64_t output_len = compressed.size() - compressed_size;
                uint8_t *output = compressed.data() + compressed_size;
                RESULT_OK_AND_ASSIGN(result, compressor->End(output_len, output));
                ASSERT_LE(result.bytes_written, output_len);
                compressed_size += result.bytes_written;
                if (result.should_retry) {
                    compressed.resize(compressed.capacity() * 2);
                }
            } while (result.should_retry);

            compressed.resize(compressed_size);
        }

        // Then decompress...

        std::vector<uint8_t> decompressed(2);
        int64_t decompressed_size = 0;
        {
            const uint8_t *input = compressed.data();
            int64_t remaining = compressed.size();

            while (!decompressor->IsFinished()) {
                // Feed a varying amount each time
                int64_t input_len = std::min(remaining, make_buf_size());
                int64_t output_len = decompressed.size() - decompressed_size;
                uint8_t *output = decompressed.data() + decompressed_size;
                RESULT_OK_AND_ASSIGN(
                        auto result, decompressor->Decompress(input_len, input, output_len, output));
                ASSERT_LE(result.bytes_read, input_len);
                ASSERT_LE(result.bytes_written, output_len);
                ASSERT_TRUE(result.need_more_output || result.bytes_written > 0 ||
                            result.bytes_read > 0)
                                            << "Decompression not progressing anymore";
                if (result.need_more_output) {
                    decompressed.resize(decompressed.capacity() * 2);
                }
                decompressed_size += result.bytes_written;
                input += result.bytes_read;
                remaining -= result.bytes_read;
            }
            ASSERT_EQ(remaining, 0);
            decompressed.resize(decompressed_size);
        }

        ASSERT_EQ(data.size(), decompressed.size());
        ASSERT_EQ(data, decompressed);
    }

    void CheckStreamingRoundtrip(Codec *codec, const std::vector<uint8_t> &data) {
        std::shared_ptr<Compressor> compressor;
        std::shared_ptr<Decompressor> decompressor;
        RESULT_OK_AND_ASSIGN(compressor, codec->MakeCompressor());
        RESULT_OK_AND_ASSIGN(decompressor, codec->MakeDecompressor());

        CheckStreamingRoundtrip(compressor, decompressor, data);
    }

    class CodecTest : public ::testing::TestWithParam<CompressionType> {
    protected:
        CompressionType GetCompression() { return GetParam(); }

        std::unique_ptr<Codec> MakeCodec() { return *Codec::Create(GetCompression()); }
    };

    TEST(TestCodecMisc, GetCodecAsString) {
        EXPECT_EQ(Codec::GetCodecAsString(CompressionType::UNCOMPRESSED), "uncompressed");
        EXPECT_EQ(Codec::GetCodecAsString(CompressionType::SNAPPY), "snappy");
        EXPECT_EQ(Codec::GetCodecAsString(CompressionType::GZIP), "gzip");
        EXPECT_EQ(Codec::GetCodecAsString(CompressionType::LZO), "lzo");
        EXPECT_EQ(Codec::GetCodecAsString(CompressionType::BROTLI), "brotli");
        EXPECT_EQ(Codec::GetCodecAsString(CompressionType::LZ4), "lz4_raw");
        EXPECT_EQ(Codec::GetCodecAsString(CompressionType::LZ4_FRAME), "lz4");
        EXPECT_EQ(Codec::GetCodecAsString(CompressionType::ZSTD), "zstd");
        EXPECT_EQ(Codec::GetCodecAsString(CompressionType::BZ2), "bz2");
    }

    TEST(TestCodecMisc, GetCompressionType) {
        ASSERT_OK_AND_EQ(CompressionType::UNCOMPRESSED, Codec::GetCompressionType("uncompressed"));
        ASSERT_OK_AND_EQ(CompressionType::SNAPPY, Codec::GetCompressionType("snappy"));
        ASSERT_OK_AND_EQ(CompressionType::GZIP, Codec::GetCompressionType("gzip"));
        ASSERT_OK_AND_EQ(CompressionType::LZO, Codec::GetCompressionType("lzo"));
        ASSERT_OK_AND_EQ(CompressionType::BROTLI, Codec::GetCompressionType("brotli"));
        ASSERT_OK_AND_EQ(CompressionType::LZ4, Codec::GetCompressionType("lz4_raw"));
        ASSERT_OK_AND_EQ(CompressionType::LZ4_FRAME, Codec::GetCompressionType("lz4"));
        ASSERT_OK_AND_EQ(CompressionType::ZSTD, Codec::GetCompressionType("zstd"));
        ASSERT_OK_AND_EQ(CompressionType::BZ2, Codec::GetCompressionType("bz2"));

        ASSERT_EQ(Codec::GetCompressionType("unk").status().code(), turbo::StatusCode::kInvalidArgument);
        ASSERT_EQ(Codec::GetCompressionType("SNAPPY").status().code(), turbo::StatusCode::kInvalidArgument);
    }

    TEST_P(CodecTest, CodecRoundtrip) {
        const auto compression = GetCompression();
        if (compression == CompressionType::BZ2) {
            GTEST_SKIP() << "BZ2 does not support one-shot compression";
        }

        int sizes[] = {0, 10000, 100000};

        // create multiple compressors to try to break them
        std::unique_ptr<Codec> c1, c2;
        RESULT_OK_AND_ASSIGN(c1, Codec::Create(compression));
        RESULT_OK_AND_ASSIGN(c2, Codec::Create(compression));

        for (int data_size: sizes) {
            std::vector<uint8_t> data = MakeRandomData(data_size);
            CheckCodecRoundtrip(c1, c2, data);

            data = MakeCompressibleData(data_size);
            CheckCodecRoundtrip(c1, c2, data);
        }
    }

    TEST(CodecTest, CodecRoundtripGzipMembers) {
#ifndef ENABLE_ZLIB
        GTEST_SKIP() << "Test requires Zlib compression";
#endif
        std::unique_ptr<Codec> gzip_codec;
        RESULT_OK_AND_ASSIGN(gzip_codec, Codec::Create(CompressionType::GZIP));

        for (int data_size: {0, 10000, 100000}) {
            int64_t compressed_size_p1, compressed_size_p2;
            uint32_t p1_size = data_size / 4;
            uint32_t p2_size = data_size - p1_size;
            std::vector<uint8_t> data_full = MakeRandomData(data_size);
            std::vector<uint8_t> data_p1(data_full.begin(), data_full.begin() + p1_size);
            std::vector<uint8_t> data_p2(data_full.begin() + p1_size, data_full.end());

            int max_compressed_len_p1 =
                    static_cast<int>(gzip_codec->MaxCompressedLen(p1_size, data_p1.data()));
            int max_compressed_len_p2 =
                    static_cast<int>(gzip_codec->MaxCompressedLen(p2_size, data_p2.data()));
            std::vector<uint8_t> compressed(max_compressed_len_p1 + max_compressed_len_p2);

            // Compress in 2 parts separately
            RESULT_OK_AND_ASSIGN(compressed_size_p1,
                                 gzip_codec->Compress(p1_size, data_p1.data(),
                                                      max_compressed_len_p1, compressed.data()));
            RESULT_OK_AND_ASSIGN(
                    compressed_size_p2,
                    gzip_codec->Compress(p2_size, data_p2.data(), max_compressed_len_p2,
                                         compressed.data() + compressed_size_p1));
            compressed.resize(compressed_size_p1 + compressed_size_p2);

            // Decompress the concatenated compressed gzip members
            std::vector<uint8_t> decompressed(data_size);
            int64_t actual_decompressed_size;
            RESULT_OK_AND_ASSIGN(
                    actual_decompressed_size,
                    gzip_codec->Decompress(compressed.size(), compressed.data(), decompressed.size(),
                                           decompressed.data()));

            ASSERT_EQ(data_size, actual_decompressed_size);
            ASSERT_EQ(data_full, decompressed);
        }
    }

    TEST(TestCodecMisc, SpecifyCompressionLevel) {
        struct CombinationOption {
            CompressionType codec;
            int level;
            bool expect_success;
        };
        constexpr CombinationOption combinations[] = {
                {CompressionType::GZIP,         2,    true},
                {CompressionType::BROTLI,       10,   true},
                {CompressionType::ZSTD,         4,    true},
                {CompressionType::LZ4,          10,   true},
                {CompressionType::LZO,          -22,  false},
                {CompressionType::UNCOMPRESSED, 10,   false},
                {CompressionType::SNAPPY,       16,   false},
                {CompressionType::GZIP,         -992, false},
                {CompressionType::LZ4_FRAME,    9,    true}};

        std::vector<uint8_t> data = MakeRandomData(2000);
        for (const auto &combination: combinations) {
            const auto compression = combination.codec;
            if (!Codec::IsAvailable(compression)) {
                // Support for this codec hasn't been built
                continue;
            }
            const auto level = combination.level;
            const auto codec_options = alkaid::CodecOptions(level);
            const auto expect_success = combination.expect_success;
            auto result1 = Codec::Create(compression, codec_options);
            auto result2 = Codec::Create(compression, codec_options);
            ASSERT_EQ(expect_success, result1.ok());
            ASSERT_EQ(expect_success, result2.ok());
            if (expect_success) {
                CheckCodecRoundtrip(*result1, *result2, data);
            }
        }
    }

    TEST(TestCodecMisc, SpecifyCodecOptionsGZip) {
        // for now only GZIP & Brotli codec options supported, since it has specific parameters
        // to be customized, other codecs could directly go with CodecOptions, could add more
        // specific codec options if needed.
        struct CombinationOption {
            int level;
            GZipFormat format;
            int window_bits;
            bool expect_success;
        };
        constexpr CombinationOption combinations[] = {{2,    GZipFormat::ZLIB,    12,  true},
                                                      {9,    GZipFormat::GZIP,    9,   true},
                                                      {9,    GZipFormat::GZIP,    20,  false},
                                                      {5,    GZipFormat::DEFLATE, -12, false},
                                                      {-992, GZipFormat::GZIP,    15,  false}};

        std::vector<uint8_t> data = MakeRandomData(2000);
        for (const auto &combination: combinations) {
            const auto compression = CompressionType::GZIP;
            if (!Codec::IsAvailable(compression)) {
                // Support for this codec hasn't been built
                continue;
            }
            auto codec_options = alkaid::GZipCodecOptions();
            codec_options.compression_level = combination.level;
            codec_options.gzip_format = combination.format;
            codec_options.window_bits = combination.window_bits;
            const auto expect_success = combination.expect_success;
            auto result1 = Codec::Create(compression, codec_options);
            auto result2 = Codec::Create(compression, codec_options);
            ASSERT_EQ(expect_success, result1.ok());
            ASSERT_EQ(expect_success, result2.ok());
            if (expect_success) {
                CheckCodecRoundtrip(*result1, *result2, data);
            }
        }
    }

    TEST(TestCodecMisc, SpecifyCodecOptionsBrotli) {
        // for now only GZIP & Brotli codec options supported, since it has specific parameters
        // to be customized, other codecs could directly go with CodecOptions, could add more
        // specific codec options if needed.
        struct CombinationOption {
            int level;
            int window_bits;
            bool expect_success;
        };
        constexpr CombinationOption combinations[] = {
                {8,    22,  true},
                {11,   10,  true},
                {1,    24,  true},
                {5,    -12, false},
                {-992, 25,  false}};

        std::vector<uint8_t> data = MakeRandomData(2000);
        for (const auto &combination: combinations) {
            const auto compression = CompressionType::BROTLI;
            if (!Codec::IsAvailable(compression)) {
                // Support for this codec hasn't been built
                continue;
            }
            auto codec_options = alkaid::BrotliCodecOptions();
            codec_options.compression_level = combination.level;
            codec_options.window_bits = combination.window_bits;
            const auto expect_success = combination.expect_success;
            auto result1 = Codec::Create(compression, codec_options);
            auto result2 = Codec::Create(compression, codec_options);
            ASSERT_EQ(expect_success, result1.ok());
            ASSERT_EQ(expect_success, result2.ok());
            if (expect_success) {
                CheckCodecRoundtrip(*result1, *result2, data);
            }
        }
    }

    TEST_P(CodecTest, MinMaxCompressionLevel) {
        auto type = GetCompression();
        RESULT_OK_AND_ASSIGN(auto codec, Codec::Create(type));

        if (Codec::SupportsCompressionLevel(type)) {
            RESULT_OK_AND_ASSIGN(auto min_level, Codec::MinimumCompressionLevel(type));
            RESULT_OK_AND_ASSIGN(auto max_level, Codec::MaximumCompressionLevel(type));
            RESULT_OK_AND_ASSIGN(auto default_level, Codec::DefaultCompressionLevel(type));
            ASSERT_NE(min_level, Codec::UseDefaultCompressionLevel());
            ASSERT_NE(max_level, Codec::UseDefaultCompressionLevel());
            ASSERT_NE(default_level, Codec::UseDefaultCompressionLevel());
            ASSERT_LT(min_level, max_level);
            ASSERT_EQ(min_level, codec->minimum_compression_level());
            ASSERT_EQ(max_level, codec->maximum_compression_level());
            ASSERT_GE(default_level, min_level);
            ASSERT_LE(default_level, max_level);
        } else {
            ASSERT_EQ(turbo::StatusCode::kInvalidArgument, Codec::MinimumCompressionLevel(type).status().code());
            ASSERT_EQ(turbo::StatusCode::kInvalidArgument, Codec::MaximumCompressionLevel(type).status().code());
            ASSERT_EQ(turbo::StatusCode::kInvalidArgument, Codec::DefaultCompressionLevel(type).status().code());
            ASSERT_EQ(codec->minimum_compression_level(), Codec::UseDefaultCompressionLevel());
            ASSERT_EQ(codec->maximum_compression_level(), Codec::UseDefaultCompressionLevel());
            ASSERT_EQ(codec->default_compression_level(), Codec::UseDefaultCompressionLevel());
        }
    }

    TEST_P(CodecTest, OutputBufferIsSmall) {
        auto type = GetCompression();
        if (type != CompressionType::SNAPPY) {
            return;
        }

        RESULT_OK_AND_ASSIGN(auto codec, Codec::Create(type));

        std::vector<uint8_t> data = MakeRandomData(10);
        auto max_compressed_len = codec->MaxCompressedLen(data.size(), data.data());
        std::vector<uint8_t> compressed(max_compressed_len);
        std::vector<uint8_t> decompressed(data.size() - 1);

        int64_t actual_size;
        RESULT_OK_AND_ASSIGN(
                actual_size,
                codec->Compress(data.size(), data.data(), max_compressed_len, compressed.data()));
        compressed.resize(actual_size);

        std::stringstream ss;
        ss << "Invalid: Output buffer size (" << decompressed.size() << ") must be "
           << data.size() << " or larger.";
        ASSERT_EQ(codec->Decompress(compressed.size(), compressed.data(),
                                                     decompressed.size(), decompressed.data()).status().code(), turbo::StatusCode::kInvalidArgument);
    }

    TEST_P(CodecTest, StreamingCompressor) {
        if (GetCompression() == CompressionType::SNAPPY) {
            GTEST_SKIP() << "snappy doesn't support streaming compression";
        }
        if (GetCompression() == CompressionType::BZ2) {
            GTEST_SKIP() << "Z2 doesn't support one-shot decompression";
        }
        if (GetCompression() == CompressionType::LZ4 ||
            GetCompression() == CompressionType::LZ4_HADOOP) {
            GTEST_SKIP() << "LZ4 raw format doesn't support streaming compression.";
        }

        int sizes[] = {0, 10, 100000};
        for (int data_size: sizes) {
            auto codec = MakeCodec();

            std::vector<uint8_t> data = MakeRandomData(data_size);
            CheckStreamingCompressor(codec.get(), data);

            data = MakeCompressibleData(data_size);
            CheckStreamingCompressor(codec.get(), data);
        }
    }

    TEST_P(CodecTest, StreamingDecompressor) {
        if (GetCompression() == CompressionType::SNAPPY) {
            GTEST_SKIP() << "snappy doesn't support streaming decompression.";
        }
        if (GetCompression() == CompressionType::BZ2) {
            GTEST_SKIP() << "Z2 doesn't support one-shot compression";
        }
        if (GetCompression() == CompressionType::LZ4 ||
            GetCompression() == CompressionType::LZ4_HADOOP) {
            GTEST_SKIP() << "LZ4 raw format doesn't support streaming decompression.";
        }

        int sizes[] = {0, 10, 100000};
        for (int data_size: sizes) {
            auto codec = MakeCodec();

            std::vector<uint8_t> data = MakeRandomData(data_size);
            CheckStreamingDecompressor(codec.get(), data);

            data = MakeCompressibleData(data_size);
            CheckStreamingDecompressor(codec.get(), data);
        }
    }

    TEST_P(CodecTest, StreamingRoundtrip) {
        if (GetCompression() == CompressionType::SNAPPY) {
            GTEST_SKIP() << "snappy doesn't support streaming decompression";
        }
        if (GetCompression() == CompressionType::LZ4 ||
            GetCompression() == CompressionType::LZ4_HADOOP) {
            GTEST_SKIP() << "LZ4 raw format doesn't support streaming compression.";
        }

        int sizes[] = {0, 10, 100000};
        for (int data_size: sizes) {
            auto codec = MakeCodec();

            std::vector<uint8_t> data = MakeRandomData(data_size);
            CheckStreamingRoundtrip(codec.get(), data);

            data = MakeCompressibleData(data_size);
            CheckStreamingRoundtrip(codec.get(), data);
        }
    }

    TEST_P(CodecTest, StreamingDecompressorReuse) {
        if (GetCompression() == CompressionType::SNAPPY) {
            GTEST_SKIP() << "snappy doesn't support streaming decompression";
        }
        if (GetCompression() == CompressionType::LZ4 ||
            GetCompression() == CompressionType::LZ4_HADOOP) {
            GTEST_SKIP() << "LZ4 raw format doesn't support streaming decompression.";
        }

        auto codec = MakeCodec();
        std::shared_ptr<Compressor> compressor;
        std::shared_ptr<Decompressor> decompressor;
        RESULT_OK_AND_ASSIGN(compressor, codec->MakeCompressor());
        RESULT_OK_AND_ASSIGN(decompressor, codec->MakeDecompressor());

        std::vector<uint8_t> data = MakeRandomData(100);
        CheckStreamingRoundtrip(compressor, decompressor, data);
        // Decompressor::Reset() should allow reusing decompressor for a new stream
        RESULT_OK_AND_ASSIGN(compressor, codec->MakeCompressor());
        ASSERT_TRUE(decompressor->Reset().ok());
        data = MakeRandomData(200);
        CheckStreamingRoundtrip(compressor, decompressor, data);
    }

    TEST_P(CodecTest, StreamingMultiFlush) {
        if (GetCompression() == CompressionType::SNAPPY) {
            GTEST_SKIP() << "snappy doesn't support streaming decompression";
        }
        if (GetCompression() == CompressionType::LZ4 ||
            GetCompression() == CompressionType::LZ4_HADOOP) {
            GTEST_SKIP() << "LZ4 raw format doesn't support streaming decompression.";
        }
        auto type = GetCompression();
        RESULT_OK_AND_ASSIGN(auto codec, Codec::Create(type));

        std::shared_ptr<Compressor> compressor;
        RESULT_OK_AND_ASSIGN(compressor, codec->MakeCompressor());

        // Grow the buffer and flush again while requested (up to a bounded number of times)
        std::vector<uint8_t> compressed(1024);
        Compressor::FlushResult result;
        int attempts = 0;
        int64_t actual_size = 0;
        int64_t output_len = 0;
        uint8_t *output = compressed.data();
        do {
            compressed.resize(compressed.capacity() * 2);
            output_len = compressed.size() - actual_size;
            output = compressed.data() + actual_size;
            RESULT_OK_AND_ASSIGN(result, compressor->Flush(output_len, output));
            actual_size += result.bytes_written;
            attempts++;
        } while (attempts < 8 && result.should_retry);
        // The LZ4 codec actually needs this many attempts to settle

        // Flush again having done nothing - should not require retry
        output_len = compressed.size() - actual_size;
        output = compressed.data() + actual_size;
        RESULT_OK_AND_ASSIGN(result, compressor->Flush(output_len, output));
        ASSERT_FALSE(result.should_retry);
    }

#if !defined ENABLE_ZLIB && !defined ENABLE_SNAPPY && !defined ENABLE_LZ4 && \
    !defined ENABLE_BROTLI && !defined ENABLE_BZIP2 && !defined ENABLE_ZSTD
    GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(CodecTest);
#endif

#ifdef ENABLE_ZLIB
    INSTANTIATE_TEST_SUITE_P(TestGZip, CodecTest, ::testing::Values(CompressionType::GZIP));
#endif

#ifdef ENABLE_SNAPPY
    INSTANTIATE_TEST_SUITE_P(TestSnappy, CodecTest, ::testing::Values(CompressionType::SNAPPY));
#endif

#ifdef ENABLE_LZ4
    INSTANTIATE_TEST_SUITE_P(TestLZ4, CodecTest, ::testing::Values(CompressionType::LZ4));
    INSTANTIATE_TEST_SUITE_P(TestLZ4Hadoop, CodecTest,
                             ::testing::Values(CompressionType::LZ4_HADOOP));
#endif

#ifdef ENABLE_LZ4
    INSTANTIATE_TEST_SUITE_P(TestLZ4Frame, CodecTest,
                             ::testing::Values(CompressionType::LZ4_FRAME));
#endif

#ifdef ENABLE_BROTLI
    INSTANTIATE_TEST_SUITE_P(TestBrotli, CodecTest, ::testing::Values(CompressionType::BROTLI));
#endif

#ifdef ENABLE_BZIP2
    INSTANTIATE_TEST_SUITE_P(TestBZ2, CodecTest, ::testing::Values(CompressionType::BZ2));
#endif

#ifdef ENABLE_ZSTD
    INSTANTIATE_TEST_SUITE_P(TestZSTD, CodecTest, ::testing::Values(CompressionType::ZSTD));
#endif

#ifdef ENABLE_LZ4
    TEST(TestCodecLZ4Hadoop, Compatibility) {
      // LZ4 Hadoop codec should be able to read back LZ4 raw blocks
      RESULT_OK_AND_ASSIGN(auto c1, Codec::Create(CompressionType::LZ4));
      RESULT_OK_AND_ASSIGN(auto c2, Codec::Create(CompressionType::LZ4_HADOOP));

      std::vector<uint8_t> data = MakeRandomData(100);
      CheckCodecRoundtrip(c1, c2, data, /*check_reverse=*/false);
    }
#endif

}  // namespace alkaid
