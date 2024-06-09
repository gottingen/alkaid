#
# Copyright 2023 The titan-search Authors.
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
###########################################################################
# below are examples, after initialization, modify it to your own.
###########################################################################
###carbin_example
set(ALKAID_SRC
        files/interface.cc
        files/local/sys_io.cc
        files/local/sequential_read_file.cc
        files/local/sequential_write_file.cc
        files/local/random_read_file.cc
        files/local/random_write_file.cc
        files/local/temp_file.cc
        files/local/sequential_read_mmap_file.cc
        files/local/random_read_mmap_file.cc
        files/localfs.cc
        csv/format.cc
        csv/row.cc
)
carbin_cc_library(
        NAMESPACE alkaid
        NAME alkaid
        SOURCES
        ${ALKAID_SRC}
        CXXOPTS
        ${CARBIN_CXX_OPTIONS}
        PLINKS
        ${CARBIN_DEPS_LINK}
        PUBLIC
)
