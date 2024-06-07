// Copyright 2023 The Elastic-AI Authors.
// part of Elastic AI Search
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//
#pragma once
#include <cstddef>
#include <cstring>
#include <iomanip>
#include <fstream>
#include <turbo/container/flat_hash_map.h>
#include <turbo/utility/status.h>
#include <turbo/log/logging.h>
#include <turbo/strings/str_split.h>
#include <turbo/strings/str_format.h>
#include <turbo/strings/str_cat.h>
#include <turbo/strings/substitute.h>
#include <turbo/strings/str_split.h>
#include <cstdint>

namespace polaris {
    class PropertySet : public turbo::flat_hash_map<std::string, std::string> {
    public:
        void set(const std::string &key, const std::string &value) {
            auto it = find(key);
            if (it == end()) {
                insert(std::pair<std::string, std::string>(key, value));
            } else {
                (*it).second = value;
            }
        }

        template<class T>
        void set(const std::string_view &key, T value) {
            auto str = turbo::str_cat(value);
            auto it = find(key);
            if (it == end()) {
                insert(std::pair<std::string, std::string>(key, str));
            } else {
                (*it).second = str;
            }
        }

        std::string get(const std::string &key) const {
            auto it = find(key);
            if (it != end()) {
                return it->second;
            }
            return "";
        }

        void updateAndInsert(PropertySet &prop) {
            for (auto i = prop.begin(); i != prop.end(); ++i) {
                set((*i).first, (*i).second);
            }
        }

        [[nodiscard]] turbo::Status load(const std::string &f) {
            std::ifstream st(f);
            if (!st) {
                return turbo::errno_to_status(errno, turbo::substitute("PropertySet::load: Cannot load the property file $0", f));
            }
            return load(st);
        }

        [[nodiscard]] turbo::Status save(const std::string &f) {
            std::ofstream st(f);
            if (!st) {
                return turbo::errno_to_status(errno, turbo::substitute("PropertySet::save: Cannot save. $0", f));
            }
            return save(st);
        }

        [[nodiscard]] turbo::Status save(std::ofstream &os) const {

            try {
                for (auto i = this->begin(); i != this->end(); i++) {
                    os << i->first << "\t" << i->second << std::endl;
                }
            } catch (std::exception &e) {
                return turbo::errno_to_status(errno, turbo::substitute("PropertySet::save: $0", e.what()));
            }
            return turbo::OkStatus();
        }

        [[nodiscard]] turbo::Status load(std::ifstream &is) {
            std::string line;
            try {
            while (getline(is, line)) {
                std::vector<std::string_view> tokens = turbo::str_split(line, "\t");
                if (tokens.size() != 2) {
                    std::cerr << "Property file is illegal. " << line << std::endl;
                    continue;
                }
                set(tokens[0], tokens[1]);
            }
            } catch (std::exception &e) {
                return turbo::errno_to_status(errno, turbo::substitute("PropertySet::load: $0", e.what()));
            }
            return turbo::OkStatus();
        }
    };

}  // namespace polaris
