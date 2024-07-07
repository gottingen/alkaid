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
// Created by jeff on 24-7-7.
//

#pragma once

#include <gtest/gtest.h>
#include <random>
#include <alkaid/files/filesystem.h>

//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// Behaviour Switches (should match the config in ghc/filesystem.hpp):
//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// LWG #2682 disables the since then invalid use of the copy option create_symlinks on directories
#define TEST_LWG_2682_BEHAVIOUR
//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// LWG #2395 makes crate_directory/create_directories not emit an error if there is a regular
// file with that name, it is superceded by P1164R1, so only activate if really needed
// #define TEST_LWG_2935_BEHAVIOUR
//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// LWG #2937 enforces that fs::equivalent emits an error, if !fs::exists(p1)||!exists(p2)
#define TEST_LWG_2937_BEHAVIOUR
//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

static bool has_host_root_name_support() {
    return alkaid::FilePath("//host").has_root_name();
}


template <class T>
class TestAllocator
{
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using difference_type = ptrdiff_t;
    using size_type = size_t;
    TestAllocator() noexcept {}
    template <class U>
    TestAllocator(TestAllocator<U> const&) noexcept
    {
    }
    value_type* allocate(std::size_t n) { return static_cast<value_type*>(::operator new(n * sizeof(value_type))); }
    void deallocate(value_type* p, std::size_t) noexcept { ::operator delete(p); }
    template<class U>
    struct rebind {
        typedef TestAllocator<U> other;
    };
};

template <class T, class U>
static bool operator==(TestAllocator<T> const&, TestAllocator<U> const&) noexcept
{
    return true;
}

template <class T, class U>
static bool operator!=(TestAllocator<T> const& x, TestAllocator<U> const& y) noexcept
{
    return !(x == y);
}


#ifdef ALKAID_OS_WINDOWS
#if !defined(_WIN64) && defined(KEY_WOW64_64KEY)
static bool isWow64Proc()
{
    typedef BOOL(WINAPI * IsWow64Process_t)(HANDLE, PBOOL);
    BOOL bIsWow64 = FALSE;
    auto fnIsWow64Process = (IsWow64Process_t)GetProcAddress(GetModuleHandle(TEXT("kernel32")), "IsWow64Process");
    if (NULL != fnIsWow64Process) {
        if (!fnIsWow64Process(GetCurrentProcess(), &bIsWow64)) {
            bIsWow64 = FALSE;
        }
    }
    return bIsWow64 == TRUE;
}
#endif

static bool is_symlink_creation_supported()
{
    bool result = true;
    HKEY key;
    REGSAM flags = KEY_READ;
#ifdef _WIN64
    flags |= KEY_WOW64_64KEY;
#elif defined(KEY_WOW64_64KEY)
    if (isWow64Proc()) {
        flags |= KEY_WOW64_64KEY;
    }
    else {
        flags |= KEY_WOW64_32KEY;
    }
#else
    result = false;
#endif
    if (result) {
        auto err = RegOpenKeyExW(HKEY_LOCAL_MACHINE, L"SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\AppModelUnlock", 0, flags, &key);
        if (err == ERROR_SUCCESS) {
            DWORD val = 0, size = sizeof(DWORD);
            err = RegQueryValueExW(key, L"AllowDevelopmentWithoutDevLicense", 0, NULL, reinterpret_cast<LPBYTE>(&val), &size);
            RegCloseKey(key);
            if (err != ERROR_SUCCESS) {
                result = false;
            }
            else {
                result = (val != 0);
            }
        }
        else {
            result = false;
        }
    }
    if (!result) {
        std::clog << "Warning: Symlink creation not supported." << std::endl;
    }
    return result;
}
#else

static bool is_symlink_creation_supported() {
    return true;
}

#endif

#define RESULT_OK_AND_TRUE(x) \
    ASSERT_TRUE(x.ok()) << x.status().message(); \
    ASSERT_TRUE(x.value())

#define RESULT_OK_AND_FALSE(x) \
    ASSERT_TRUE(x.ok()) << x.status().message(); \
    ASSERT_TRUE(!x.value())

#define RESULT_OK_AND_EQ(x, y) \
    ASSERT_TRUE(x.ok()) << x.status().message(); \
    ASSERT_TRUE(x.value() == y)

template<typename T>
bool result_eq(const turbo::Result<T> &r, const T &v) {
    return r.ok() && r.value() == v;
}

enum class TempOpt {
    none, change_path
};

static void generateFile(const alkaid::FilePath &pathname, int withSize = -1) {
    alkaid::ofstream outfile(pathname);
    if (withSize < 0) {
        outfile << "Hello world!" << std::endl;
    } else {
        outfile << std::string(size_t(withSize), '*');
    }
}

class TemporaryDirectory {
public:
    TemporaryDirectory(TempOpt opt = TempOpt::none) {
        static auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
        static auto rng = std::bind(std::uniform_int_distribution<int>(0, 35), std::mt19937(
                static_cast<unsigned int>(seed) ^ static_cast<unsigned int>(reinterpret_cast<ptrdiff_t>(&opt))));
        std::string filename;
        do {
            filename = "test_";
            for (int i = 0; i < 8; ++i) {
                filename += "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"[rng()];
            }
            _path = alkaid::canonical(alkaid::temp_directory_path().value()).value() / filename;
        } while (alkaid::exists(_path).value());
        (void) alkaid::create_directories(_path);
        if (opt == TempOpt::change_path) {
            _orig_dir = alkaid::current_path().value();
            (void) alkaid::current_path(_path);
        }
    }

    ~TemporaryDirectory() {
        if (!_orig_dir.empty()) {
            (void) alkaid::current_path(_orig_dir);
        }
        (void) alkaid::remove_all(_path);
    }

    const alkaid::FilePath &path() const { return _path; }

private:
    alkaid::FilePath _path;
    alkaid::FilePath _orig_dir;
};

