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
#pragma once

#include <memory>

#include <alkaid/compress/compression.h>  // IWYU pragma: export

namespace alkaid::internal {

    // Brotli compression quality is max (11) by default, which is slow.
    // We use 8 as a default as it is the best trade-off for Parquet workload.
    constexpr int kBrotliDefaultCompressionLevel = 8;

    // Brotli codec.
    std::unique_ptr<Codec> MakeBrotliCodec(
            int compression_level = kBrotliDefaultCompressionLevel,
            std::optional<int> window_bits = std::nullopt);

    // BZ2 codec.
    constexpr int kBZ2DefaultCompressionLevel = 9;

    std::unique_ptr<Codec> MakeBZ2Codec(int compression_level = kBZ2DefaultCompressionLevel);

    // GZip
    constexpr int kGZipDefaultCompressionLevel = 9;

    std::unique_ptr<Codec> MakeGZipCodec(int compression_level = kGZipDefaultCompressionLevel,
                                         GZipFormat format = GZipFormat::GZIP,
                                         std::optional<int> window_bits = std::nullopt);

    // Snappy
    std::unique_ptr<Codec> MakeSnappyCodec();

    // Lz4 Codecs
    constexpr int kLz4DefaultCompressionLevel = 1;

    // Lz4 frame format codec.

    std::unique_ptr<Codec> MakeLz4FrameCodec(
            int compression_level = kLz4DefaultCompressionLevel);

    // Lz4 "raw" format codec.
    std::unique_ptr<Codec> MakeLz4RawCodec(
            int compression_level = kLz4DefaultCompressionLevel);

    // Lz4 "Hadoop" format codec (== Lz4 raw codec prefixed with lengths header)
    std::unique_ptr<Codec> MakeLz4HadoopRawCodec();

    // ZSTD codec.

    // XXX level = 1 probably doesn't compress very much
    constexpr int kZSTDDefaultCompressionLevel = 1;

    std::unique_ptr<Codec> MakeZSTDCodec(
            int compression_level = kZSTDDefaultCompressionLevel);

}  // namespace alkaid::internal
