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

#include <alkaid/compress/compression.h>

#include <memory>
#include <string>
#include <utility>

#include <alkaid/compress/compression_internal.h>
#include <turbo/log/logging.h>

namespace alkaid {
    namespace {

        turbo::Status CheckSupportsCompressionLevel(CompressionType type) {
            if (!Codec::SupportsCompressionLevel(type)) {
                return turbo::invalid_argument_error(
                        "The specified codec does not support the compression level parameter");
            }
            return turbo::OkStatus();
        }

    }  // namespace

    int Codec::UseDefaultCompressionLevel() { return kUseDefaultCompressionLevel; }

    turbo::Status Codec::Init() { return turbo::OkStatus(); }

    const std::string &Codec::GetCodecAsString(CompressionType t) {
        static const std::string uncompressed = "uncompressed", snappy = "snappy",
                gzip = "gzip", lzo = "lzo", brotli = "brotli",
                lz4_raw = "lz4_raw", lz4 = "lz4", lz4_hadoop = "lz4_hadoop",
                zstd = "zstd", bz2 = "bz2", unknown = "unknown";

        switch (t) {
            case CompressionType::UNCOMPRESSED:
                return uncompressed;
            case CompressionType::SNAPPY:
                return snappy;
            case CompressionType::GZIP:
                return gzip;
            case CompressionType::LZO:
                return lzo;
            case CompressionType::BROTLI:
                return brotli;
            case CompressionType::LZ4:
                return lz4_raw;
            case CompressionType::LZ4_FRAME:
                return lz4;
            case CompressionType::LZ4_HADOOP:
                return lz4_hadoop;
            case CompressionType::ZSTD:
                return zstd;
            case CompressionType::BZ2:
                return bz2;
            default:
                return unknown;
        }
    }

    turbo::Result<CompressionType> Codec::GetCompressionType(const std::string &name) {
        if (name == "uncompressed") {
            return CompressionType::UNCOMPRESSED;
        } else if (name == "gzip") {
            return CompressionType::GZIP;
        } else if (name == "snappy") {
            return CompressionType::SNAPPY;
        } else if (name == "lzo") {
            return CompressionType::LZO;
        } else if (name == "brotli") {
            return CompressionType::BROTLI;
        } else if (name == "lz4_raw") {
            return CompressionType::LZ4;
        } else if (name == "lz4") {
            return CompressionType::LZ4_FRAME;
        } else if (name == "lz4_hadoop") {
            return CompressionType::LZ4_HADOOP;
        } else if (name == "zstd") {
            return CompressionType::ZSTD;
        } else if (name == "bz2") {
            return CompressionType::BZ2;
        } else {
            return turbo::invalid_argument_error("Unrecognized compression type: %s", name);
        }
    }

    bool Codec::SupportsCompressionLevel(CompressionType codec) {
        switch (codec) {
            case CompressionType::GZIP:
            case CompressionType::BROTLI:
            case CompressionType::ZSTD:
            case CompressionType::BZ2:
            case CompressionType::LZ4_FRAME:
            case CompressionType::LZ4:
                return true;
            default:
                return false;
        }
    }

    turbo::Result<int> Codec::MaximumCompressionLevel(CompressionType codec_type) {
        STATUS_RETURN_IF_ERROR(CheckSupportsCompressionLevel(codec_type));
        RESULT_ASSIGN_OR_RETURN(auto codec, Codec::Create(codec_type));
        return codec->maximum_compression_level();
    }

    turbo::Result<int> Codec::MinimumCompressionLevel(CompressionType codec_type) {
        STATUS_RETURN_IF_ERROR(CheckSupportsCompressionLevel(codec_type));
        RESULT_ASSIGN_OR_RETURN(auto codec, Codec::Create(codec_type));
        return codec->minimum_compression_level();
    }

    turbo::Result<int> Codec::DefaultCompressionLevel(CompressionType codec_type) {
        STATUS_RETURN_IF_ERROR(CheckSupportsCompressionLevel(codec_type));
        RESULT_ASSIGN_OR_RETURN(auto codec, Codec::Create(codec_type));
        return codec->default_compression_level();
    }

    turbo::Result<std::unique_ptr<Codec>> Codec::Create(CompressionType codec_type,
                                                        const CodecOptions &codec_options) {
        if (!IsAvailable(codec_type)) {
            if (codec_type == CompressionType::LZO) {
                return turbo::unimplemented_error("LZO codec not implemented");
            }

            auto name = GetCodecAsString(codec_type);
            if (name == "unknown") {
                return turbo::invalid_argument_error("Unrecognized codec");
            }

            return turbo::unimplemented_error(
                    turbo::str_cat("Support for codec '", GetCodecAsString(codec_type), "' not built"));
        }

        auto compression_level = codec_options.compression_level;
        if (compression_level != kUseDefaultCompressionLevel &&
            !SupportsCompressionLevel(codec_type)) {
            return turbo::invalid_argument_error(turbo::str_cat("Codec '", GetCodecAsString(codec_type),
                                                                "' doesn't support setting a compression level."));
        }

        std::unique_ptr<Codec> codec;
        switch (codec_type) {
            case CompressionType::UNCOMPRESSED:
                return nullptr;
            case CompressionType::SNAPPY:
#ifdef ENABLE_SNAPPY
                codec = internal::MakeSnappyCodec();
#endif
                break;
            case CompressionType::GZIP: {
#ifdef ENABLE_ZLIB
                auto opt = dynamic_cast<const GZipCodecOptions*>(&codec_options);
                codec = internal::MakeGZipCodec(compression_level,
                                                opt ? opt->gzip_format : GZipFormat::GZIP,
                                                opt ? opt->window_bits : std::nullopt);
#endif
                break;
            }
            case CompressionType::BROTLI: {
#ifdef ENABLE_BROTLI
                auto opt = dynamic_cast<const BrotliCodecOptions*>(&codec_options);
                codec = internal::MakeBrotliCodec(compression_level,
                                                  opt ? opt->window_bits : std::nullopt);
#endif
                break;
            }
            case CompressionType::LZ4:
#ifdef ENABLE_LZ4
                codec = internal::MakeLz4RawCodec(compression_level);
#endif
                break;
            case CompressionType::LZ4_FRAME:
#ifdef ENABLE_LZ4
                codec = internal::MakeLz4FrameCodec(compression_level);
#endif
                break;
            case CompressionType::LZ4_HADOOP:
#ifdef ENABLE_LZ4
                codec = internal::MakeLz4HadoopRawCodec();
#endif
                break;
            case CompressionType::ZSTD:
#ifdef ENABLE_ZSTD
                codec = internal::MakeZSTDCodec(compression_level);
#endif
                break;
            case CompressionType::BZ2:
#ifdef ENABLE_BZIP2
                codec = internal::MakeBZ2Codec(compression_level);
#endif
                break;
            default:
                break;
        }

        DCHECK_NE(codec.get(), nullptr);
        STATUS_RETURN_IF_ERROR(codec->Init());
        return codec;
    }

    // use compression level to create Codec
    turbo::Result<std::unique_ptr<Codec>> Codec::Create(CompressionType codec_type,
                                                        int compression_level) {
        return Codec::Create(codec_type, CodecOptions{compression_level});
    }

    bool Codec::IsAvailable(CompressionType codec_type) {
        switch (codec_type) {
            case CompressionType::UNCOMPRESSED:
                return true;
            case CompressionType::SNAPPY:
#ifdef ENABLE_SNAPPY
                return true;
#else
                return false;
#endif
            case CompressionType::GZIP:
#ifdef ENABLE_ZLIB
                return true;
#else
                return false;
#endif
            case CompressionType::LZO:
                return false;
            case CompressionType::BROTLI:
#ifdef ENABLE_BROTLI
                return true;
#else
                return false;
#endif
            case CompressionType::LZ4:
            case CompressionType::LZ4_FRAME:
            case CompressionType::LZ4_HADOOP:
#ifdef ENABLE_LZ4
                return true;
#else
                return false;
#endif
            case CompressionType::ZSTD:
#ifdef ENABLE_ZSTD
                return true;
#else
                return false;
#endif
            case CompressionType::BZ2:
#ifdef ENABLE_BZIP2
                return true;
#else
                return false;
#endif
            default:
                return false;
        }
    }

}  // namespace alkaid
