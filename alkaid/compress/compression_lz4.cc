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

#include <alkaid/version.h>

#if defined(ENABLE_LZ4)

#include <alkaid/compress/compression_internal.h>
#include <turbo/log/logging.h>
#include <turbo/strings/str_cat.h>
#include <turbo/base/endian.h>
#include <cstdint>
#include <cstring>
#include <memory>

#include <lz4.h>
#include <lz4frame.h>
#include <lz4hc.h>


#ifndef LZ4F_HEADER_SIZE_MAX
#define LZ4F_HEADER_SIZE_MAX 19
#endif

namespace alkaid::internal {

    namespace {

        constexpr int kLz4MinCompressionLevel = 1;

        static turbo::Status LZ4Error(LZ4F_errorCode_t ret, const char *prefix_msg) {
            return turbo::unavailable_error(turbo::str_cat(prefix_msg, LZ4F_getErrorName(ret)));
        }

        static LZ4F_preferences_t DefaultPreferences() {
            LZ4F_preferences_t prefs;
            memset(&prefs, 0, sizeof(prefs));
            return prefs;
        }

        static LZ4F_preferences_t PreferencesWithCompressionLevel(int compression_level) {
            LZ4F_preferences_t prefs = DefaultPreferences();
            prefs.compressionLevel = compression_level;
            return prefs;
        }

// ----------------------------------------------------------------------
// Lz4 frame decompressor implementation

        class LZ4Decompressor : public Decompressor {
        public:
            LZ4Decompressor() {}

            ~LZ4Decompressor() override {
                if (ctx_ != nullptr) {
                    (void) (LZ4F_freeDecompressionContext(ctx_));
                }
            }

            turbo::Status Init() {
                LZ4F_errorCode_t ret;
                finished_ = false;

                ret = LZ4F_createDecompressionContext(&ctx_, LZ4F_VERSION);
                if (LZ4F_isError(ret)) {
                    return LZ4Error(ret, "LZ4 init failed: ");
                } else {
                    return turbo::OkStatus();
                }
            }

            turbo::Status Reset() override {
#if defined(LZ4_VERSION_NUMBER) && LZ4_VERSION_NUMBER >= 10800
                // LZ4F_resetDecompressionContext appeared in 1.8.0
                DCHECK_NE(ctx_, nullptr);
                LZ4F_resetDecompressionContext(ctx_);
                finished_ = false;
                return turbo::OkStatus();
#else
                if (ctx_ != nullptr) {
                  ARROW_UNUSED(LZ4F_freeDecompressionContext(ctx_));
                }
                return Init();
#endif
            }

            turbo::Result<DecompressResult> Decompress(int64_t input_len, const uint8_t *input,
                                                       int64_t output_len, uint8_t *output) override {
                auto src = input;
                auto dst = output;
                auto src_size = static_cast<size_t>(input_len);
                auto dst_capacity = static_cast<size_t>(output_len);
                size_t ret;

                DCHECK_NE(src, nullptr);
                ret =
                        LZ4F_decompress(ctx_, dst, &dst_capacity, src, &src_size, nullptr /* options */);
                if (LZ4F_isError(ret)) {
                    return LZ4Error(ret, "LZ4 decompress failed: ");
                }
                finished_ = (ret == 0);
                return DecompressResult{static_cast<int64_t>(src_size),
                                        static_cast<int64_t>(dst_capacity),
                                        (src_size == 0 && dst_capacity == 0)};
            }

            bool IsFinished() override { return finished_; }

        protected:
            LZ4F_decompressionContext_t ctx_ = nullptr;
            bool finished_;
        };

// ----------------------------------------------------------------------
// Lz4 frame compressor implementation

        class LZ4Compressor : public Compressor {
        public:
            explicit LZ4Compressor(int compression_level) : compression_level_(compression_level) {}

            ~LZ4Compressor() override {
                if (ctx_ != nullptr) {
                    (void) (LZ4F_freeCompressionContext(ctx_));
                }
            }

            turbo::Status Init() {
                LZ4F_errorCode_t ret;
                prefs_ = PreferencesWithCompressionLevel(compression_level_);
                first_time_ = true;

                ret = LZ4F_createCompressionContext(&ctx_, LZ4F_VERSION);
                if (LZ4F_isError(ret)) {
                    return LZ4Error(ret, "LZ4 init failed: ");
                } else {
                    return turbo::OkStatus();
                }
            }

#define BEGIN_COMPRESS(dst, dst_capacity, output_too_small)     \
  if (first_time_) {                                            \
    if (dst_capacity < LZ4F_HEADER_SIZE_MAX) {                  \
      /* Output too small to write LZ4F header */               \
      return (output_too_small);                                \
    }                                                           \
    ret = LZ4F_compressBegin(ctx_, dst, dst_capacity, &prefs_); \
    if (LZ4F_isError(ret)) {                                    \
      return LZ4Error(ret, "LZ4 compress begin failed: ");      \
    }                                                           \
    first_time_ = false;                                        \
    dst += ret;                                                 \
    dst_capacity -= ret;                                        \
    bytes_written += static_cast<int64_t>(ret);                 \
  }

            turbo::Result<CompressResult> Compress(int64_t input_len, const uint8_t *input,
                                                   int64_t output_len, uint8_t *output) override {
                auto src = input;
                auto dst = output;
                auto src_size = static_cast<size_t>(input_len);
                auto dst_capacity = static_cast<size_t>(output_len);
                size_t ret;
                int64_t bytes_written = 0;

                BEGIN_COMPRESS(dst, dst_capacity, (CompressResult{0, 0}));

                if (dst_capacity < LZ4F_compressBound(src_size, &prefs_)) {
                    // Output too small to compress into
                    return CompressResult{0, bytes_written};
                }
                ret = LZ4F_compressUpdate(ctx_, dst, dst_capacity, src, src_size,
                                          nullptr /* options */);
                if (LZ4F_isError(ret)) {
                    return LZ4Error(ret, "LZ4 compress update failed: ");
                }
                bytes_written += static_cast<int64_t>(ret);
                DCHECK_LE(bytes_written, output_len);
                return CompressResult{input_len, bytes_written};
            }

            turbo::Result<FlushResult> Flush(int64_t output_len, uint8_t *output) override {
                auto dst = output;
                auto dst_capacity = static_cast<size_t>(output_len);
                size_t ret;
                int64_t bytes_written = 0;

                BEGIN_COMPRESS(dst, dst_capacity, (FlushResult{0, true}));

                if (dst_capacity < LZ4F_compressBound(0, &prefs_)) {
                    // Output too small to flush into
                    return FlushResult{bytes_written, true};
                }

                ret = LZ4F_flush(ctx_, dst, dst_capacity, nullptr /* options */);
                if (LZ4F_isError(ret)) {
                    return LZ4Error(ret, "LZ4 flush failed: ");
                }
                bytes_written += static_cast<int64_t>(ret);
                DCHECK_LE(bytes_written, output_len);
                return FlushResult{bytes_written, false};
            }

            turbo::Result<EndResult> End(int64_t output_len, uint8_t *output) override {
                auto dst = output;
                auto dst_capacity = static_cast<size_t>(output_len);
                size_t ret;
                int64_t bytes_written = 0;

                BEGIN_COMPRESS(dst, dst_capacity, (EndResult{0, true}));

                if (dst_capacity < LZ4F_compressBound(0, &prefs_)) {
                    // Output too small to end frame into
                    return EndResult{bytes_written, true};
                }

                ret = LZ4F_compressEnd(ctx_, dst, dst_capacity, nullptr /* options */);
                if (LZ4F_isError(ret)) {
                    return LZ4Error(ret, "LZ4 end failed: ");
                }
                bytes_written += static_cast<int64_t>(ret);
                DCHECK_LE(bytes_written, output_len);
                return EndResult{bytes_written, false};
            }

#undef BEGIN_COMPRESS

        protected:
            int compression_level_;
            LZ4F_compressionContext_t ctx_ = nullptr;
            LZ4F_preferences_t prefs_;
            bool first_time_;
        };

// ----------------------------------------------------------------------
// Lz4 frame codec implementation

        class Lz4FrameCodec : public Codec {
        public:
            explicit Lz4FrameCodec(int compression_level)
                    : compression_level_(compression_level == kUseDefaultCompressionLevel
                                         ? kLz4DefaultCompressionLevel
                                         : compression_level),
                      prefs_(PreferencesWithCompressionLevel(compression_level_)) {}

            int64_t MaxCompressedLen(int64_t input_len,
                                     const uint8_t *input) override {
                (void) input;
                return static_cast<int64_t>(
                        LZ4F_compressFrameBound(static_cast<size_t>(input_len), &prefs_));
            }

            turbo::Result<int64_t> Compress(int64_t input_len, const uint8_t *input,
                                            int64_t output_buffer_len, uint8_t *output_buffer) override {
                auto output_len =
                        LZ4F_compressFrame(output_buffer, static_cast<size_t>(output_buffer_len), input,
                                           static_cast<size_t>(input_len), &prefs_);
                if (LZ4F_isError(output_len)) {
                    return LZ4Error(output_len, "Lz4 compression failure: ");
                }
                return static_cast<int64_t>(output_len);
            }

            turbo::Result<int64_t> Decompress(int64_t input_len, const uint8_t *input,
                                              int64_t output_buffer_len, uint8_t *output_buffer) override {
                RESULT_ASSIGN_OR_RETURN(auto decomp, MakeDecompressor());

                int64_t total_bytes_written = 0;
                while (!decomp->IsFinished() && input_len != 0) {
                    RESULT_ASSIGN_OR_RETURN(
                            auto res,
                            decomp->Decompress(input_len, input, output_buffer_len, output_buffer));
                    input += res.bytes_read;
                    input_len -= res.bytes_read;
                    output_buffer += res.bytes_written;
                    output_buffer_len -= res.bytes_written;
                    total_bytes_written += res.bytes_written;
                    if (res.need_more_output) {
                        return turbo::unavailable_error("Lz4 decompression buffer too small");
                    }
                }
                if (!decomp->IsFinished()) {
                    return turbo::unavailable_error("Lz4 compressed input contains less than one frame");
                }
                if (input_len != 0) {
                    return turbo::unavailable_error("Lz4 compressed input contains more than one frame");
                }
                return total_bytes_written;
            }

            turbo::Result<std::shared_ptr<Compressor>> MakeCompressor() override {
                auto ptr = std::make_shared<LZ4Compressor>(compression_level_);
                STATUS_RETURN_IF_ERROR(ptr->Init());
                return ptr;
            }

            turbo::Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
                auto ptr = std::make_shared<LZ4Decompressor>();
                STATUS_RETURN_IF_ERROR(ptr->Init());
                return ptr;
            }

            CompressionType compression_type() const override { return CompressionType::LZ4_FRAME; }

            int minimum_compression_level() const override { return kLz4MinCompressionLevel; }

#if (defined(LZ4_VERSION_NUMBER) && LZ4_VERSION_NUMBER < 10800)
            int maximum_compression_level() const override { return 12; }
#else

            int maximum_compression_level() const override { return LZ4F_compressionLevel_max(); }

#endif

            int default_compression_level() const override { return kLz4DefaultCompressionLevel; }

            int compression_level() const override { return compression_level_; }

        protected:
            const int compression_level_;
            const LZ4F_preferences_t prefs_;
        };

// ----------------------------------------------------------------------
// Lz4 "raw" codec implementation

        class Lz4Codec : public Codec {
        public:
            explicit Lz4Codec(int compression_level)
                    : compression_level_(compression_level == kUseDefaultCompressionLevel
                                         ? kLz4DefaultCompressionLevel
                                         : compression_level) {}

            turbo::Result<int64_t> Decompress(int64_t input_len, const uint8_t *input,
                                              int64_t output_buffer_len, uint8_t *output_buffer) override {
                int64_t decompressed_size = LZ4_decompress_safe(
                        reinterpret_cast<const char *>(input), reinterpret_cast<char *>(output_buffer),
                        static_cast<int>(input_len), static_cast<int>(output_buffer_len));
                if (decompressed_size < 0) {
                    return turbo::unavailable_error("Corrupt Lz4 compressed data.");
                }
                return decompressed_size;
            }

            int64_t MaxCompressedLen(int64_t input_len,
                                     const uint8_t *input) override {
                (void) input;
                return LZ4_compressBound(static_cast<int>(input_len));
            }

            turbo::Result<int64_t> Compress(int64_t input_len, const uint8_t *input,
                                            int64_t output_buffer_len, uint8_t *output_buffer) override {
                int64_t output_len;
#ifdef LZ4HC_CLEVEL_MIN
                constexpr int min_hc_clevel = LZ4HC_CLEVEL_MIN;
#else  // For older versions of the lz4 library
                constexpr int min_hc_clevel = 3;
#endif
                if (compression_level_ < min_hc_clevel) {
                    output_len = LZ4_compress_default(
                            reinterpret_cast<const char *>(input), reinterpret_cast<char *>(output_buffer),
                            static_cast<int>(input_len), static_cast<int>(output_buffer_len));
                } else {
                    output_len = LZ4_compress_HC(
                            reinterpret_cast<const char *>(input), reinterpret_cast<char *>(output_buffer),
                            static_cast<int>(input_len), static_cast<int>(output_buffer_len),
                            compression_level_);
                }
                if (output_len == 0) {
                    return turbo::unavailable_error("Lz4 compression failure.");
                }
                return output_len;
            }

            turbo::Result<std::shared_ptr<Compressor>> MakeCompressor() override {
                return turbo::unimplemented_error(
                        "Streaming compression unsupported with LZ4 raw format. "
                        "Try using LZ4 frame format instead.");
            }

            turbo::Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
                return turbo::unimplemented_error(
                        "Streaming decompression unsupported with LZ4 raw format. "
                        "Try using LZ4 frame format instead.");
            }

            CompressionType compression_type() const override { return CompressionType::LZ4; }

            int minimum_compression_level() const override { return kLz4MinCompressionLevel; }

#if (defined(LZ4_VERSION_NUMBER) && LZ4_VERSION_NUMBER < 10800)
            int maximum_compression_level() const override { return 12; }
#else

            int maximum_compression_level() const override { return LZ4F_compressionLevel_max(); }

#endif

            int default_compression_level() const override { return kLz4DefaultCompressionLevel; }

        protected:
            int compression_level_;
        };

// ----------------------------------------------------------------------
// Lz4 Hadoop "raw" codec implementation

        class Lz4HadoopCodec : public Lz4Codec {
        public:
            Lz4HadoopCodec() : Lz4Codec(kUseDefaultCompressionLevel) {}

            turbo::Result<int64_t> Decompress(int64_t input_len, const uint8_t *input,
                                              int64_t output_buffer_len, uint8_t *output_buffer) override {
                const int64_t decompressed_size =
                        TryDecompressHadoop(input_len, input, output_buffer_len, output_buffer);
                if (decompressed_size != kNotHadoop) {
                    return decompressed_size;
                }
                // Fall back on raw LZ4 codec (for files produces by earlier versions of Parquet C++)
                return Lz4Codec::Decompress(input_len, input, output_buffer_len, output_buffer);
            }

            int64_t MaxCompressedLen(int64_t input_len,
                                     const uint8_t *input) override {
                (void) input;
                return kPrefixLength + Lz4Codec::MaxCompressedLen(input_len, nullptr);
            }

            turbo::Result<int64_t> Compress(int64_t input_len, const uint8_t *input,
                                            int64_t output_buffer_len, uint8_t *output_buffer) override {
                if (output_buffer_len < kPrefixLength) {
                    return turbo::invalid_argument_error("Output buffer too small for Lz4HadoopCodec compression");
                }

                RESULT_ASSIGN_OR_RETURN(
                        int64_t output_len,
                        Lz4Codec::Compress(input_len, input, output_buffer_len - kPrefixLength,
                                           output_buffer + kPrefixLength));

                // Prepend decompressed size in bytes and compressed size in bytes
                // to be compatible with Hadoop Lz4Codec
                const uint32_t decompressed_size = input_len;
                const uint32_t compressed_size = output_len;
                turbo::big_endian::store32(output_buffer, decompressed_size);
                turbo::big_endian::store32(output_buffer + sizeof(uint32_t), compressed_size);

                return kPrefixLength + output_len;
            }

            turbo::Result<std::shared_ptr<Compressor>> MakeCompressor() override {
                return turbo::unimplemented_error(
                        "Streaming compression unsupported with LZ4 Hadoop raw format. "
                        "Try using LZ4 frame format instead.");
            }

            turbo::Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
                return turbo::unimplemented_error(
                        "Streaming decompression unsupported with LZ4 Hadoop raw format. "
                        "Try using LZ4 frame format instead.");
            }

            CompressionType compression_type() const override { return CompressionType::LZ4_HADOOP; }

        protected:
            // Offset starting at which page data can be read/written
            static const int64_t kPrefixLength = sizeof(uint32_t) * 2;

            static const int64_t kNotHadoop = -1;

            int64_t TryDecompressHadoop(int64_t input_len, const uint8_t *input,
                                        int64_t output_buffer_len, uint8_t *output_buffer) {
                // Parquet files written with the Hadoop Lz4Codec use their own framing.
                // The input buffer can contain an arbitrary number of "frames", each
                // with the following structure:
                // - bytes 0..3: big-endian uint32_t representing the frame decompressed size
                // - bytes 4..7: big-endian uint32_t representing the frame compressed size
                // - bytes 8...: frame compressed data
                //
                // The Hadoop Lz4Codec source code can be found here:
                // https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/src/main/native/src/codec/Lz4Codec.cc
                int64_t total_decompressed_size = 0;

                while (input_len >= kPrefixLength) {
                    const uint32_t expected_decompressed_size = turbo::big_endian::load32(input);
                    const uint32_t expected_compressed_size = turbo::big_endian::load32(input + sizeof(uint32_t));
                    input += kPrefixLength;
                    input_len -= kPrefixLength;

                    if (input_len < expected_compressed_size) {
                        // Not enough bytes for Hadoop "frame"
                        return kNotHadoop;
                    }
                    if (output_buffer_len < expected_decompressed_size) {
                        // Not enough bytes to hold advertised output => probably not Hadoop
                        return kNotHadoop;
                    }
                    // Try decompressing and compare with expected decompressed length
                    auto maybe_decompressed_size = Lz4Codec::Decompress(
                            expected_compressed_size, input, output_buffer_len, output_buffer);
                    if (!maybe_decompressed_size.ok() ||
                        *maybe_decompressed_size != expected_decompressed_size) {
                        return kNotHadoop;
                    }
                    input += expected_compressed_size;
                    input_len -= expected_compressed_size;
                    output_buffer += expected_decompressed_size;
                    output_buffer_len -= expected_decompressed_size;
                    total_decompressed_size += expected_decompressed_size;
                }

                if (input_len == 0) {
                    return total_decompressed_size;
                } else {
                    return kNotHadoop;
                }
            }

            int minimum_compression_level() const override { return kUseDefaultCompressionLevel; }

            int maximum_compression_level() const override { return kUseDefaultCompressionLevel; }

            int default_compression_level() const override { return kUseDefaultCompressionLevel; }
        };

    }  // namespace

    std::unique_ptr<Codec> MakeLz4FrameCodec(int compression_level) {
        return std::make_unique<Lz4FrameCodec>(compression_level);
    }

    std::unique_ptr<Codec> MakeLz4HadoopRawCodec() {
        return std::make_unique<Lz4HadoopCodec>();
    }

    std::unique_ptr<Codec> MakeLz4RawCodec(int compression_level) {
        return std::make_unique<Lz4Codec>(compression_level);
    }

}  // namespace alkaid::internal
#endif  // defined(ENABLE_LZ4)