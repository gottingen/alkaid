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

#if defined(ENABLE_ZLIB)

#include <alkaid/compress/compression_internal.h>
#include <turbo/utility/status.h>
#include <turbo/log/logging.h>

#include <cstddef>
#include <cstdint>
#include <memory>

#include <zstd.h>


using std::size_t;

namespace alkaid::internal {

    namespace {

        turbo::Status ZSTDError(size_t ret, const char *prefix_msg) {
            return turbo::unavailable_error(turbo::str_cat(prefix_msg, ZSTD_getErrorName(ret)));
        }

// ----------------------------------------------------------------------
// ZSTD decompressor implementation

        class ZSTDDecompressor : public Decompressor {
        public:
            ZSTDDecompressor() : stream_(ZSTD_createDStream()) {}

            ~ZSTDDecompressor() override { ZSTD_freeDStream(stream_); }

            turbo::Status Init() {
                finished_ = false;
                size_t ret = ZSTD_initDStream(stream_);
                if (ZSTD_isError(ret)) {
                    return ZSTDError(ret, "ZSTD init failed: ");
                } else {
                    return turbo::OkStatus();
                }
            }

            turbo::Result<DecompressResult> Decompress(int64_t input_len, const uint8_t *input,
                                                       int64_t output_len, uint8_t *output) override {
                ZSTD_inBuffer in_buf;
                ZSTD_outBuffer out_buf;

                in_buf.src = input;
                in_buf.size = static_cast<size_t>(input_len);
                in_buf.pos = 0;
                out_buf.dst = output;
                out_buf.size = static_cast<size_t>(output_len);
                out_buf.pos = 0;

                size_t ret;
                ret = ZSTD_decompressStream(stream_, &out_buf, &in_buf);
                if (ZSTD_isError(ret)) {
                    return ZSTDError(ret, "ZSTD decompress failed: ");
                }
                finished_ = (ret == 0);
                return DecompressResult{static_cast<int64_t>(in_buf.pos),
                                        static_cast<int64_t>(out_buf.pos),
                                        in_buf.pos == 0 && out_buf.pos == 0};
            }

            turbo::Status Reset() override { return Init(); }

            bool IsFinished() override { return finished_; }

        protected:
            ZSTD_DStream *stream_;
            bool finished_;
        };

// ----------------------------------------------------------------------
// ZSTD compressor implementation

        class ZSTDCompressor : public Compressor {
        public:
            explicit ZSTDCompressor(int compression_level)
                    : stream_(ZSTD_createCStream()), compression_level_(compression_level) {}

            ~ZSTDCompressor() override { ZSTD_freeCStream(stream_); }

            turbo::Status Init() {
                size_t ret = ZSTD_initCStream(stream_, compression_level_);
                if (ZSTD_isError(ret)) {
                    return ZSTDError(ret, "ZSTD init failed: ");
                } else {
                    return turbo::OkStatus();
                }
            }

            turbo::Result<CompressResult> Compress(int64_t input_len, const uint8_t *input,
                                                   int64_t output_len, uint8_t *output) override {
                ZSTD_inBuffer in_buf;
                ZSTD_outBuffer out_buf;

                in_buf.src = input;
                in_buf.size = static_cast<size_t>(input_len);
                in_buf.pos = 0;
                out_buf.dst = output;
                out_buf.size = static_cast<size_t>(output_len);
                out_buf.pos = 0;

                size_t ret;
                ret = ZSTD_compressStream(stream_, &out_buf, &in_buf);
                if (ZSTD_isError(ret)) {
                    return ZSTDError(ret, "ZSTD compress failed: ");
                }
                return CompressResult{static_cast<int64_t>(in_buf.pos),
                                      static_cast<int64_t>(out_buf.pos)};
            }

            turbo::Result<FlushResult> Flush(int64_t output_len, uint8_t *output) override {
                ZSTD_outBuffer out_buf;

                out_buf.dst = output;
                out_buf.size = static_cast<size_t>(output_len);
                out_buf.pos = 0;

                size_t ret;
                ret = ZSTD_flushStream(stream_, &out_buf);
                if (ZSTD_isError(ret)) {
                    return ZSTDError(ret, "ZSTD flush failed: ");
                }
                return FlushResult{static_cast<int64_t>(out_buf.pos), ret > 0};
            }

            turbo::Result<EndResult> End(int64_t output_len, uint8_t *output) override {
                ZSTD_outBuffer out_buf;

                out_buf.dst = output;
                out_buf.size = static_cast<size_t>(output_len);
                out_buf.pos = 0;

                size_t ret;
                ret = ZSTD_endStream(stream_, &out_buf);
                if (ZSTD_isError(ret)) {
                    return ZSTDError(ret, "ZSTD end failed: ");
                }
                return EndResult{static_cast<int64_t>(out_buf.pos), ret > 0};
            }

        protected:
            ZSTD_CStream *stream_;

        private:
            int compression_level_;
        };

        // ----------------------------------------------------------------------
        // ZSTD codec implementation

        class ZSTDCodec : public Codec {
        public:
            explicit ZSTDCodec(int compression_level)
                    : compression_level_(compression_level == kUseDefaultCompressionLevel
                                         ? kZSTDDefaultCompressionLevel
                                         : compression_level) {}

            turbo::Result<int64_t> Decompress(int64_t input_len, const uint8_t *input,
                                              int64_t output_buffer_len, uint8_t *output_buffer) override {
                if (output_buffer == nullptr) {
                    // We may pass a NULL 0-byte output buffer but some zstd versions demand
                    // a valid pointer: https://github.com/facebook/zstd/issues/1385
                    static uint8_t empty_buffer;
                    DCHECK_EQ(output_buffer_len, 0);
                    output_buffer = &empty_buffer;
                }

                size_t ret = ZSTD_decompress(output_buffer, static_cast<size_t>(output_buffer_len),
                                             input, static_cast<size_t>(input_len));
                if (ZSTD_isError(ret)) {
                    return ZSTDError(ret, "ZSTD decompression failed: ");
                }
                if (static_cast<int64_t>(ret) != output_buffer_len) {
                    return turbo::unavailable_error("Corrupt ZSTD compressed data.");
                }
                return static_cast<int64_t>(ret);
            }

            int64_t MaxCompressedLen(int64_t input_len,
                                     const uint8_t *input) override {
                (void) input;
                DCHECK_GE(input_len, 0);
                return ZSTD_compressBound(static_cast<size_t>(input_len));
            }

            turbo::Result<int64_t> Compress(int64_t input_len, const uint8_t *input,
                                            int64_t output_buffer_len, uint8_t *output_buffer) override {
                size_t ret = ZSTD_compress(output_buffer, static_cast<size_t>(output_buffer_len),
                                           input, static_cast<size_t>(input_len), compression_level_);
                if (ZSTD_isError(ret)) {
                    return ZSTDError(ret, "ZSTD compression failed: ");
                }
                return static_cast<int64_t>(ret);
            }

            turbo::Result<std::shared_ptr<Compressor>> MakeCompressor() override {
                auto ptr = std::make_shared<ZSTDCompressor>(compression_level_);
                STATUS_RETURN_IF_ERROR(ptr->Init());
                return ptr;
            }

            turbo::Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
                auto ptr = std::make_shared<ZSTDDecompressor>();
                STATUS_RETURN_IF_ERROR(ptr->Init());
                return ptr;
            }

            CompressionType compression_type() const override { return CompressionType::ZSTD; }

            int minimum_compression_level() const override { return ZSTD_minCLevel(); }

            int maximum_compression_level() const override { return ZSTD_maxCLevel(); }

            int default_compression_level() const override { return kZSTDDefaultCompressionLevel; }

            int compression_level() const override { return compression_level_; }

        private:
            const int compression_level_;
        };

    }  // namespace

    std::unique_ptr<Codec> MakeZSTDCodec(int compression_level) {
        return std::make_unique<ZSTDCodec>(compression_level);
    }

}  // namespace alkaid::internal
#endif // ENABLE_ZSTD