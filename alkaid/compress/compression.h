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

#pragma once

#include <turbo/base/macros.h>
#include <turbo/utility/status.h>
#include <alkaid/version.h>

namespace alkaid {

    constexpr int kUseDefaultCompressionLevel = std::numeric_limits<int>::min();

    enum CompressionType {
        UNCOMPRESSED,
        SNAPPY,
        GZIP,
        BROTLI,
        ZSTD,
        LZ4,
        LZ4_FRAME,
        LZO,
        BZ2,
        LZ4_HADOOP
    };

    class TURBO_EXPORT Compressor {
    public:
        virtual ~Compressor() = default;

        struct CompressResult {
            int64_t bytes_read;
            int64_t bytes_written;
        };
        struct FlushResult {
            int64_t bytes_written;
            bool should_retry;
        };
        struct EndResult {
            int64_t bytes_written;
            bool should_retry;
        };

        /// \brief Compress some input.
        ///
        /// If bytes_read is 0 on return, then a larger output buffer should be supplied.
        virtual turbo::Result<CompressResult> Compress(int64_t input_len, const uint8_t *input,
                                                       int64_t output_len, uint8_t *output) = 0;

        /// \brief Flush part of the compressed output.
        ///
        /// If should_retry is true on return, Flush() should be called again
        /// with a larger buffer.
        virtual turbo::Result<FlushResult> Flush(int64_t output_len, uint8_t *output) = 0;

        /// \brief End compressing, doing whatever is necessary to end the stream.
        ///
        /// If should_retry is true on return, End() should be called again
        /// with a larger buffer.  Otherwise, the Compressor should not be used anymore.
        ///
        /// End() implies Flush().
        virtual turbo::Result<EndResult> End(int64_t output_len, uint8_t *output) = 0;

        // XXX add methods for buffer size heuristics?
    };

    /// \brief Streaming decompressor interface
    ///
    class TURBO_EXPORT Decompressor {
    public:
        virtual ~Decompressor() = default;

        struct DecompressResult {
            // XXX is need_more_output necessary? (Brotli?)
            int64_t bytes_read;
            int64_t bytes_written;
            bool need_more_output;
        };

        /// \brief Decompress some input.
        ///
        /// If need_more_output is true on return, a larger output buffer needs
        /// to be supplied.
        virtual turbo::Result<DecompressResult> Decompress(int64_t input_len, const uint8_t *input,
                                                           int64_t output_len, uint8_t *output) = 0;

        /// \brief Return whether the compressed stream is finished.
        ///
        /// This is a heuristic.  If true is returned, then it is guaranteed
        /// that the stream is finished.  If false is returned, however, it may
        /// simply be that the underlying library isn't able to provide the information.
        virtual bool IsFinished() = 0;

        /// \brief Reinitialize decompressor, making it ready for a new compressed stream.
        virtual turbo::Status Reset() = 0;

        // XXX add methods for buffer size heuristics?
    };

    /// \brief Compression codec options
    class TURBO_EXPORT CodecOptions {
    public:
        explicit CodecOptions(int compression_level = kUseDefaultCompressionLevel)
                : compression_level(compression_level) {}

        virtual ~CodecOptions() = default;

        int compression_level;
    };

    // ----------------------------------------------------------------------
    // GZip codec options implementation

    enum class GZipFormat {
        ZLIB,
        DEFLATE,
        GZIP,
    };

    class TURBO_EXPORT GZipCodecOptions : public CodecOptions {
    public:
        GZipFormat gzip_format = GZipFormat::GZIP;
        std::optional<int> window_bits;
    };

    // ----------------------------------------------------------------------
    // brotli codec options implementation

    class TURBO_EXPORT BrotliCodecOptions : public CodecOptions {
    public:
        std::optional<int> window_bits;
    };

    /// \brief Compression codec
    class TURBO_EXPORT Codec {
    public:
        virtual ~Codec() = default;

        /// \brief Return special value to indicate that a codec implementation
        /// should use its default compression level
        static int UseDefaultCompressionLevel();

        /// \brief Return a string name for compression type
        static const std::string &GetCodecAsString(CompressionType t);

        /// \brief Return compression type for name (all lower case)
        static turbo::Result<CompressionType> GetCompressionType(const std::string &name);

        /// \brief Create a codec for the given compression algorithm with CodecOptions
        static turbo::Result<std::unique_ptr<Codec>> Create(
                CompressionType codec, const CodecOptions &codec_options = CodecOptions{});

        /// \brief Create a codec for the given compression algorithm
        static turbo::Result<std::unique_ptr<Codec>> Create(CompressionType codec,
                                                            int compression_level);

        /// \brief Return true if support for indicated codec has been enabled
        static bool IsAvailable(CompressionType codec);

        /// \brief Return true if indicated codec supports setting a compression level
        static bool SupportsCompressionLevel(CompressionType codec);

        /// \brief Return the smallest supported compression level for the codec
        /// Note: This function creates a temporary Codec instance
        static turbo::Result<int> MinimumCompressionLevel(CompressionType codec);

        /// \brief Return the largest supported compression level for the codec
        /// Note: This function creates a temporary Codec instance
        static turbo::Result<int> MaximumCompressionLevel(CompressionType codec);

        /// \brief Return the default compression level
        /// Note: This function creates a temporary Codec instance
        static turbo::Result<int> DefaultCompressionLevel(CompressionType codec);

        /// \brief Return the smallest supported compression level
        virtual int minimum_compression_level() const = 0;

        /// \brief Return the largest supported compression level
        virtual int maximum_compression_level() const = 0;

        /// \brief Return the default compression level
        virtual int default_compression_level() const = 0;

        /// \brief One-shot decompression function
        ///
        /// output_buffer_len must be correct and therefore be obtained in advance.
        /// The actual decompressed length is returned.
        ///
        /// \note One-shot decompression is not always compatible with streaming
        /// compression.  Depending on the codec (e.g. LZ4), different formats may
        /// be used.
        virtual turbo::Result<int64_t> Decompress(int64_t input_len, const uint8_t *input,
                                                  int64_t output_buffer_len,
                                                  uint8_t *output_buffer) = 0;

        /// \brief One-shot compression function
        ///
        /// output_buffer_len must first have been computed using MaxCompressedLen().
        /// The actual compressed length is returned.
        ///
        /// \note One-shot compression is not always compatible with streaming
        /// decompression.  Depending on the codec (e.g. LZ4), different formats may
        /// be used.
        virtual turbo::Result<int64_t> Compress(int64_t input_len, const uint8_t *input,
                                                int64_t output_buffer_len, uint8_t *output_buffer) = 0;

        virtual int64_t MaxCompressedLen(int64_t input_len, const uint8_t *input) = 0;

        /// \brief Create a streaming compressor instance
        virtual turbo::Result<std::shared_ptr<Compressor>> MakeCompressor() = 0;

        /// \brief Create a streaming compressor instance
        virtual turbo::Result<std::shared_ptr<Decompressor>> MakeDecompressor() = 0;

        /// \brief This Codec's compression type
        virtual CompressionType compression_type() const = 0;

        /// \brief The name of this Codec's compression type
        const std::string &name() const { return GetCodecAsString(compression_type()); }

        /// \brief This Codec's compression level, if applicable
        virtual int compression_level() const { return UseDefaultCompressionLevel(); }

    private:
        /// \brief Initializes the codec's resources.
        virtual turbo::Status Init();
    };

}  // namespace alkaid
