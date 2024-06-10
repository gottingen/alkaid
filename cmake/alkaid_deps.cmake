#
# Copyright 2023 The Carbin Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
############################################################
# system pthread and rt, dl
############################################################

list(APPEND CMAKE_PREFIX_PATH "/opt/EA/inf")

set(CARBIN_SYSTEM_DYLINK)
if (APPLE)
    find_library(CoreFoundation CoreFoundation)
    list(APPEND CARBIN_SYSTEM_DYLINK ${CoreFoundation} pthread)
elseif (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    list(APPEND CARBIN_SYSTEM_DYLINK rt dl pthread)
endif ()

if (CARBIN_BUILD_TEST)
    enable_testing()
    #include(require_gtest)
    #include(require_gmock)
    #include(require_doctest)
endif (CARBIN_BUILD_TEST)

if (CARBIN_BUILD_BENCHMARK)
    #include(require_benchmark)
endif ()
include(GNUInstallDirs)
set(EA_ROOT "/opt/EA/inf")
set(EA_LIB "${EA_ROOT}/lib")
set(EA_INCLUDE "${EA_ROOT}/include")
list(APPEND CMAKE_PREFIX_PATH ${EA_ROOT})

find_package(Threads REQUIRED)
find_package(turbo REQUIRED)
get_target_property(TURBO_STATIC_LIB turbo::turbo_static LOCATION)
include_directories(${turbo_INCLUDE_DIR})
set(ALKAID_COMPRESS_LIBRARIES)
################################################
# find zlib
find_library(ZLIB_LIBRARY NAMES z libz.a HINTS ${EA_LIB})
find_path(ZLIB_INCLUDE_DIR zlib.h HINTS ${EA_INCLUDE})
if(ZLIB_LIBRARY AND ZLIB_INCLUDE_DIR)
    set(ENABLE_ZLIB TRUE)
    list(APPEND ALKAID_COMPRESS_LIBRARIES ${ZLIB_LIBRARY})
endif()
# at least one compression library is required
if(NOT ENABLE_ZLIB)
    message(FATAL_ERROR "zlib not found")
endif()

################################################
# find lz4
find_library(LZ4_LIBRARY NAMES lz4 liblz4.a HINTS ${EA_LIB})
find_path(LZ4_INCLUDE_DIR lz4.h HINTS ${EA_INCLUDE})
if(LZ4_LIBRARY AND LZ4_INCLUDE_DIR)
    set(ENABLE_LZ4 TRUE)
    list(APPEND ALKAID_COMPRESS_LIBRARIES ${LZ4_LIBRARY})
endif()

################################################
# find snappy
find_library(SNAPPY_LIBRARY NAMES snappy libsnappy.a HINTS ${EA_LIB})
find_path(SNAPPY_INCLUDE_DIR snappy.h HINTS ${EA_INCLUDE})
if(SNAPPY_LIBRARY AND SNAPPY_INCLUDE_DIR)
    set(ENABLE_SNAPPY TRUE)
    list(APPEND ALKAID_COMPRESS_LIBRARIES ${SNAPPY_LIBRARY})
endif()

################################################
# find zstd
find_library(ZSTD_LIBRARY NAMES zstd libzstd.a HINTS ${EA_LIB})
find_path(ZSTD_INCLUDE_DIR zstd.h HINTS ${EA_INCLUDE})
if(ZSTD_LIBRARY AND ZSTD_INCLUDE_DIR)
    set(ENABLE_ZSTD TRUE)
    list(APPEND ALKAID_COMPRESS_LIBRARIES ${ZSTD_LIBRARY})
endif()

################################################
# find bzip2
find_library(BZIP2_LIBRARY NAMES bz2 libbz2.a HINTS ${EA_LIB})
find_path(BZIP2_INCLUDE_DIR bzlib.h HINTS ${EA_INCLUDE})
if(BZIP2_LIBRARY AND BZIP2_INCLUDE_DIR)
    set(ENABLE_BZIP2 TRUE)
    list(APPEND ALKAID_COMPRESS_LIBRARIES ${BZIP2_LIBRARY})
endif()

message(STATUS "ENABLE_ZLIB: ${ENABLE_ZLIB}")
message(STATUS "ENABLE_LZ4: ${ENABLE_LZ4}")
message(STATUS "ENABLE_SNAPPY: ${ENABLE_SNAPPY}")
message(STATUS "ENABLE_ZSTD: ${ENABLE_ZSTD}")
message(STATUS "ENABLE_BZIP2: ${ENABLE_BZIP2}")



############################################################
#
# add you libs to the CARBIN_DEPS_LINK variable eg as turbo
# so you can and system pthread and rt, dl already add to
# CARBIN_SYSTEM_DYLINK, using it for fun.
##########################################################
set(CARBIN_DEPS_LINK
        ${TURBO_STATIC_LIB}
        ${ALKAID_COMPRESS_LIBRARIES}
        ${CARBIN_SYSTEM_DYLINK}
        )
list(REMOVE_DUPLICATES CARBIN_DEPS_LINK)
carbin_print_list_label("Denpendcies:" CARBIN_DEPS_LINK)





