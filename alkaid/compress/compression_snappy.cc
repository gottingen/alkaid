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
#ifdef ENABLE_SNAPPY
#include <alkaid/compress/compression_internal.h>
#include <cstddef>
#include <cstdint>
#include <memory>

#include <snappy.h>

#include <turbo/utility/status.h>
#include <turbo/log/logging.h>

using std::size_t;

namespace alkaid::internal {

    namespace {

// ----------------------------------------------------------------------
// Snappy implementation

        class SnappyCodec : public Codec {
        public:
            turbo::Result<int64_t> Decompress(int64_t input_len, const uint8_t *input,
                                              int64_t output_buffer_len, uint8_t *output_buffer) override {
                size_t decompressed_size;
                if (!snappy::GetUncompressedLength(reinterpret_cast<const char *>(input),
                                                   static_cast<size_t>(input_len),
                                                   &decompressed_size)) {
                    return turbo::unavailable_error("Corrupt snappy compressed data.");
                }
                if (output_buffer_len < static_cast<int64_t>(decompressed_size)) {
                    return turbo::invalid_argument_error(
                            turbo::str_cat("Output buffer size (", output_buffer_len, ") must be ",
                                           decompressed_size, " or larger."));
                }
                if (!snappy::RawUncompress(reinterpret_cast<const char *>(input),
                                           static_cast<size_t>(input_len),
                                           reinterpret_cast<char *>(output_buffer))) {
                    return turbo::unavailable_error("Corrupt snappy compressed data.");
                }
                return static_cast<int64_t>(decompressed_size);
            }

            int64_t MaxCompressedLen(int64_t input_len,
                                     const uint8_t *input) override {
                (void) input;
                DCHECK_GE(input_len, 0);
                return snappy::MaxCompressedLength(static_cast<size_t>(input_len));
            }

            turbo::Result<int64_t> Compress(int64_t input_len, const uint8_t *input,
                                            int64_t output_buffer_len,
                                            uint8_t *output_buffer) override {
                (void) output_buffer_len;
                size_t output_size;
                snappy::RawCompress(reinterpret_cast<const char *>(input),
                                    static_cast<size_t>(input_len),
                                    reinterpret_cast<char *>(output_buffer), &output_size);
                return static_cast<int64_t>(output_size);
            }

            turbo::Result<std::shared_ptr<Compressor>> MakeCompressor() override {
                return turbo::unimplemented_error("Streaming compression unsupported with Snappy");
            }

            turbo::Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
                return turbo::unimplemented_error("Streaming decompression unsupported with Snappy");
            }

            CompressionType compression_type() const override { return CompressionType::SNAPPY; }

            int minimum_compression_level() const override { return kUseDefaultCompressionLevel; }

            int maximum_compression_level() const override { return kUseDefaultCompressionLevel; }

            int default_compression_level() const override { return kUseDefaultCompressionLevel; }
        };

    }  // namespace

    std::unique_ptr<Codec> MakeSnappyCodec() { return std::make_unique<SnappyCodec>(); }

}  // namespace alkaid::internal
#endif // ENABLE_SNAPPY