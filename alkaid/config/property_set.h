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
#include <map>
#include <iomanip>
#include <fstream>
#include <collie/utility/result.h>
#include <collie/log/logging.h>
#include <collie/strings/str_split.h>
#include <collie/strings/format.h>
#include <cstdint>

namespace polaris {
    class PropertySet : public std::map<std::string, std::string> {
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
        void set(const std::string &key, T value) {
            auto str = collie::format("{}", value);
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

        [[nodiscard]] collie::Status load(const std::string &f) {
            std::ifstream st(f);
            if (!st) {
                return collie::Status::from_errno(errno, "PropertySet::load: Cannot load the property file {}", f);
            }
            return load(st);
        }

        [[nodiscard]] collie::Status save(const std::string &f) {
            std::ofstream st(f);
            if (!st) {
                return collie::Status::from_errno(errno, "PropertySet::save: Cannot save. {}", f);
            }
            return save(st);
        }

        [[nodiscard]] collie::Status save(std::ofstream &os) const {

            try {
                for (auto i = this->begin(); i != this->end(); i++) {
                    os << i->first << "\t" << i->second << std::endl;
                }
            } catch (std::exception &e) {
                return collie::Status::from_errno(errno, "PropertySet::save: {}", e.what());
            }
            return collie::Status::ok_status();
        }

        [[nodiscard]] collie::Status load(std::ifstream &is) {
            std::string line;
            try {
            while (getline(is, line)) {
                std::vector<std::string> tokens = collie::str_split(line, "\t");
                if (tokens.size() != 2) {
                    std::cerr << "Property file is illegal. " << line << std::endl;
                    continue;
                }
                set(tokens[0], tokens[1]);
            }
            } catch (std::exception &e) {
                return collie::Status::from_errno(errno, "PropertySet::load: {}", e.what());
            }
            return collie::Status::ok_status();
        }
    };

}  // namespace polaris
