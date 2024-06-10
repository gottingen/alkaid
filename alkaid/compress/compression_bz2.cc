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

#if defined(ENABLE_BZIP2)

#include <alkaid/compress/compression_internal.h>
#include <turbo/utility/status.h>
#include <turbo/log/logging.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <bzlib.h>


namespace alkaid::internal {

    namespace {

        constexpr int kBZ2MinCompressionLevel = 1;
        constexpr int kBZ2MaxCompressionLevel = 9;

// Max number of bytes the bz2 APIs accept at a time
        constexpr auto kSizeLimit =
                static_cast<int64_t>(std::numeric_limits<unsigned int>::max());

        turbo::Status BZ2Error(const char *prefix_msg, int bz_result) {
            CHECK(bz_result != BZ_OK && bz_result != BZ_RUN_OK && bz_result != BZ_FLUSH_OK &&
                  bz_result != BZ_FINISH_OK && bz_result != BZ_STREAM_END);
            turbo::StatusCode code;
            std::stringstream ss;
            ss << prefix_msg;
            switch (bz_result) {
                case BZ_CONFIG_ERROR:
                    code = turbo::StatusCode::kUnknown;
                    ss << "bz2 library improperly configured (internal error)";
                    break;
                case BZ_SEQUENCE_ERROR:
                    code = turbo::StatusCode::kUnknown;
                    ss << "wrong sequence of calls to bz2 library (internal error)";
                    break;
                case BZ_PARAM_ERROR:
                    code = turbo::StatusCode::kUnknown;
                    ss << "wrong parameter to bz2 library (internal error)";
                    break;
                case BZ_MEM_ERROR:
                    code = turbo::StatusCode::kOutOfRange;
                    ss << "could not allocate memory for bz2 library";
                    break;
                case BZ_DATA_ERROR:
                    code = turbo::StatusCode::kDataLoss;
                    ss << "invalid bz2 data";
                    break;
                case BZ_DATA_ERROR_MAGIC:
                    code = turbo::StatusCode::kDataLoss;
                    ss << "data is not bz2-compressed (no magic header)";
                    break;
                default:
                    code = turbo::StatusCode::kUnknown;
                    ss << "unknown bz2 error " << bz_result;
                    break;
            }
            return turbo::Status(code, ss.str());
        }

// ----------------------------------------------------------------------
// bz2 decompressor implementation

        class BZ2Decompressor : public Decompressor {
        public:
            BZ2Decompressor() : initialized_(false) {}

            ~BZ2Decompressor() override {
                if (initialized_) {
                    (void) (BZ2_bzDecompressEnd(&stream_));
                }
            }

            turbo::Status Init() {
                DCHECK(!initialized_);
                memset(&stream_, 0, sizeof(stream_));
                int ret;
                ret = BZ2_bzDecompressInit(&stream_, 0, 0);
                if (ret != BZ_OK) {
                    return BZ2Error("bz2 decompressor init failed: ", ret);
                }
                initialized_ = true;
                finished_ = false;
                return turbo::OkStatus();
            }

            turbo::Status Reset() override {
                if (initialized_) {
                    (void) (BZ2_bzDecompressEnd(&stream_));
                    initialized_ = false;
                }
                return Init();
            }

            turbo::Result<DecompressResult> Decompress(int64_t input_len, const uint8_t *input,
                                                       int64_t output_len, uint8_t *output) override {
                stream_.next_in = const_cast<char *>(reinterpret_cast<const char *>(input));
                stream_.avail_in = static_cast<unsigned int>(std::min(input_len, kSizeLimit));
                stream_.next_out = reinterpret_cast<char *>(output);
                stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
                int ret;

                ret = BZ2_bzDecompress(&stream_);
                if (ret == BZ_OK || ret == BZ_STREAM_END) {
                    finished_ = (ret == BZ_STREAM_END);
                    int64_t bytes_read = input_len - stream_.avail_in;
                    int64_t bytes_written = output_len - stream_.avail_out;
                    return DecompressResult{bytes_read, bytes_written,
                                            (!finished_ && bytes_read == 0 && bytes_written == 0)};
                } else {
                    return BZ2Error("bz2 decompress failed: ", ret);
                }
            }

            bool IsFinished() override { return finished_; }

        protected:
            bz_stream stream_;
            bool initialized_;
            bool finished_;
        };

// ----------------------------------------------------------------------
// bz2 compressor implementation

        class BZ2Compressor : public Compressor {
        public:
            explicit BZ2Compressor(int compression_level)
                    : initialized_(false), compression_level_(compression_level) {}

            ~BZ2Compressor() override {
                if (initialized_) {
                    (void) (BZ2_bzCompressEnd(&stream_));
                }
            }

            turbo::Status Init() {
                DCHECK(!initialized_);
                memset(&stream_, 0, sizeof(stream_));
                int ret;
                ret = BZ2_bzCompressInit(&stream_, compression_level_, 0, 0);
                if (ret != BZ_OK) {
                    return BZ2Error("bz2 compressor init failed: ", ret);
                }
                initialized_ = true;
                return turbo::OkStatus();
            }

            turbo::Result<CompressResult> Compress(int64_t input_len, const uint8_t *input,
                                                   int64_t output_len, uint8_t *output) override {
                stream_.next_in = const_cast<char *>(reinterpret_cast<const char *>(input));
                stream_.avail_in = static_cast<unsigned int>(std::min(input_len, kSizeLimit));
                stream_.next_out = reinterpret_cast<char *>(output);
                stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
                int ret;

                ret = BZ2_bzCompress(&stream_, BZ_RUN);
                if (ret == BZ_RUN_OK) {
                    return CompressResult{input_len - stream_.avail_in, output_len - stream_.avail_out};
                } else {
                    return BZ2Error("bz2 compress failed: ", ret);
                }
            }

            turbo::Result<FlushResult> Flush(int64_t output_len, uint8_t *output) override {
                stream_.next_in = nullptr;
                stream_.avail_in = 0;
                stream_.next_out = reinterpret_cast<char *>(output);
                stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
                int ret;

                ret = BZ2_bzCompress(&stream_, BZ_FLUSH);
                if (ret == BZ_RUN_OK || ret == BZ_FLUSH_OK) {
                    return FlushResult{output_len - stream_.avail_out, (ret == BZ_FLUSH_OK)};
                } else {
                    return BZ2Error("bz2 compress failed: ", ret);
                }
            }

            turbo::Result<EndResult> End(int64_t output_len, uint8_t *output) override {
                stream_.next_in = nullptr;
                stream_.avail_in = 0;
                stream_.next_out = reinterpret_cast<char *>(output);
                stream_.avail_out = static_cast<unsigned int>(std::min(output_len, kSizeLimit));
                int ret;

                ret = BZ2_bzCompress(&stream_, BZ_FINISH);
                if (ret == BZ_STREAM_END || ret == BZ_FINISH_OK) {
                    return EndResult{output_len - stream_.avail_out, (ret == BZ_FINISH_OK)};
                } else {
                    return BZ2Error("bz2 compress failed: ", ret);
                }
            }

        protected:
            bz_stream stream_;
            bool initialized_;
            int compression_level_;
        };

// ----------------------------------------------------------------------
// bz2 codec implementation

        class BZ2Codec : public Codec {
        public:
            explicit BZ2Codec(int compression_level) : compression_level_(compression_level) {
                compression_level_ = compression_level == kUseDefaultCompressionLevel
                                     ? kBZ2DefaultCompressionLevel
                                     : compression_level;
            }

            turbo::Result<int64_t> Decompress(int64_t input_len, const uint8_t *input,
                                              int64_t output_buffer_len, uint8_t *output_buffer) override {
                return turbo::unimplemented_error("One-shot bz2 decompression not supported");
            }

            turbo::Result<int64_t> Compress(int64_t input_len, const uint8_t *input,
                                            int64_t output_buffer_len, uint8_t *output_buffer) override {
                return turbo::unimplemented_error("One-shot bz2 compression not supported");
            }

            int64_t MaxCompressedLen(int64_t input_len,
                                     const uint8_t *input) override {
                // Cannot determine upper bound for bz2-compressed data
                (void) input;
                (void) input_len;
                return 0;
            }

            turbo::Result<std::shared_ptr<Compressor>> MakeCompressor() override {
                auto ptr = std::make_shared<BZ2Compressor>(compression_level_);
                STATUS_RETURN_IF_ERROR(ptr->Init());
                return ptr;
            }

            turbo::Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
                auto ptr = std::make_shared<BZ2Decompressor>();
                STATUS_RETURN_IF_ERROR(ptr->Init());
                return ptr;
            }

            CompressionType compression_type() const override { return CompressionType::BZ2; }

            int compression_level() const override { return compression_level_; }

            int minimum_compression_level() const override { return kBZ2MinCompressionLevel; }

            int maximum_compression_level() const override { return kBZ2MaxCompressionLevel; }

            int default_compression_level() const override { return kBZ2DefaultCompressionLevel; }

        private:
            int compression_level_;
        };

    }  // namespace

    std::unique_ptr<Codec> MakeBZ2Codec(int compression_level) {
        return std::make_unique<BZ2Codec>(compression_level);
    }

}  // namespace alkaid::internal
#endif  // defined(ENABLE_BZIP2)