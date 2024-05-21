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
        files/internal/sys_io.cc
        files/internal/fd_utility.cc
        files/file_watcher.cc
        files/fwd.cc
        files/sequential_read_file.cc
        files/sequential_write_file.cc
        files/random_read_file.cc
        files/random_write_file.cc
        files/temp_file.cc
        files/utility.cc
        files/readline_file.cc
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
