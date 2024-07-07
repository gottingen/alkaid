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

#include <alkaid/files/filesystem.h>
#include <gtest/gtest.h>
#include "helper.h"

TEST(FilePath, preferred_separator) {
#ifdef ALKAID_OS_WINDOWS
    EXPECT_EQ(alkaid::FilePath::preferred_separator, '\\');
#else
    EXPECT_EQ(alkaid::FilePath::preferred_separator, '/');
#endif
}

#ifndef ALKAID_OS_WINDOWS
TEST(path, host) {
    if (!has_host_root_name_support()) {
        GTEST_SKIP() << "This implementation doesn't support path(\"//host\").has_root_name() == true [C++17 30.12.8.1 par. 4] on this platform, tests based on this are skipped. (Should be okay.)";
    }
}

#endif

TEST(path, construct) {
    ASSERT_TRUE("/usr/local/bin" == alkaid::FilePath("/usr/local/bin").generic_string());
    std::string str = "/usr/local/bin";
#if defined(__cpp_lib_char8_t) && !defined(ALKAID_FILESYSTEM_ENFORCE_CPP17_API)
    std::u8string u8str = u8"/usr/local/bin";
#endif
    std::u16string u16str = u"/usr/local/bin";
    std::u32string u32str = U"/usr/local/bin";
#if defined(__cpp_lib_char8_t) && !defined(ALKAID_FILESYSTEM_ENFORCE_CPP17_API)
    ASSERT_TRUE(u8str == alkaid::FilePath(u8str).generic_u8string());
#endif
    ASSERT_TRUE(u16str == alkaid::FilePath(u16str).generic_u16string());
    ASSERT_TRUE(u32str == alkaid::FilePath(u32str).generic_u32string());
    ASSERT_TRUE(str == alkaid::FilePath(str, alkaid::FilePath::format::generic_format));
    ASSERT_TRUE(str == alkaid::FilePath(str.begin(), str.end()));
    ASSERT_TRUE(alkaid::FilePath(std::wstring(3, 67)) == "CCC");
#if defined(__cpp_lib_char8_t) && !defined(ALKAID_FILESYSTEM_ENFORCE_CPP17_API)
    ASSERT_TRUE(str == alkaid::FilePath(u8str.begin(), u8str.end()));
#endif
    ASSERT_TRUE(str == alkaid::FilePath(u16str.begin(), u16str.end()));
    ASSERT_TRUE(str == alkaid::FilePath(u32str.begin(), u32str.end()));
#ifdef ALKAID_FILESYSTEM_VERSION
    ASSERT_TRUE(alkaid::FilePath("///foo/bar") == "/foo/bar");
    ASSERT_TRUE(alkaid::FilePath("//foo//bar") == "//foo/bar");
#endif
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE("\\usr\\local\\bin" == alkaid::FilePath("/usr/local/bin"));
        ASSERT_TRUE("C:\\usr\\local\\bin" == alkaid::FilePath("C:\\usr\\local\\bin"));
#else
    ASSERT_TRUE("/usr/local/bin" == alkaid::FilePath("/usr/local/bin"));
#endif
    if (has_host_root_name_support()) {
        ASSERT_TRUE("//host/foo/bar" == alkaid::FilePath("//host/foo/bar"));
    }

#if !defined(ALKAID_OS_WINDOWS) && !(defined(__GLIBCXX__) && !(defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 8))) && !defined(USE_STD_FS)
    std::locale loc;
    bool testUTF8Locale = false;
    try {
        if (const char *lang = std::getenv("LANG")) {
            loc = std::locale(lang);
        } else {
            loc = std::locale("en_US.UTF-8");
        }
        std::string name = loc.name();
        if (name.length() > 5 &&
            (name.substr(name.length() - 5) == "UTF-8" || name.substr(name.length() - 5) == "utf-8")) {
            testUTF8Locale = true;
        }
    }
    catch (std::runtime_error &) {
        GTEST_SKIP() << "Couldn't create an UTF-8 locale!";
    }
    if (testUTF8Locale) {
        auto loc_path = alkaid::FilePath::from_loc("/usr/local/bin", loc);
        ASSERT_TRUE(loc_path.ok());
        ASSERT_TRUE("/usr/local/bin" == loc_path.value());
        loc_path = alkaid::FilePath::from_loc(str.begin(), str.end(), loc);
        ASSERT_TRUE(loc_path.ok());
        ASSERT_TRUE(str == loc_path.value());
        loc_path = alkaid::FilePath::from_loc(u16str.begin(), u16str.end(), loc);
        ASSERT_TRUE(loc_path.ok());
        ASSERT_TRUE(str == loc_path.value());
        loc_path = alkaid::FilePath::from_loc(u32str.begin(), u32str.end(), loc);
        ASSERT_TRUE(loc_path.ok());
        ASSERT_TRUE(str == loc_path.value());
    }
#endif
}


TEST(path, assign) {
    alkaid::FilePath p1{"/foo/bar"};
    alkaid::FilePath p2{"/usr/local"};
    alkaid::FilePath p3;
    p3 = p1;
    ASSERT_TRUE(p1 == p3);
    p3 = alkaid::FilePath{"/usr/local"};
    ASSERT_TRUE(p2 == p3);
    p3 = alkaid::FilePath{L"/usr/local"};
    ASSERT_TRUE(p2 == p3);
    p3.assign(L"/usr/local");
    ASSERT_TRUE(p2 == p3);
#if defined(IS_WCHAR_PATH) || defined(ALKAID_USE_WCHAR_T)
    p3 = alkaid::FilePath::string_type{L"/foo/bar"};
        ASSERT_TRUE(p1 == p3);
        p3.assign(alkaid::FilePath::string_type{L"/usr/local"});
        ASSERT_TRUE(p2 == p3);
#else
    p3 = alkaid::FilePath::string_type{"/foo/bar"};
    ASSERT_TRUE(p1 == p3);
    p3.assign(alkaid::FilePath::string_type{"/usr/local"});
    ASSERT_TRUE(p2 == p3);
#endif
    p3 = std::u16string(u"/foo/bar");
    ASSERT_TRUE(p1 == p3);
    p3 = U"/usr/local";
    ASSERT_TRUE(p2 == p3);
    p3.assign(std::u16string(u"/foo/bar"));
    ASSERT_TRUE(p1 == p3);
    std::string s{"/usr/local"};
    p3.assign(s.begin(), s.end());
    ASSERT_TRUE(p2 == p3);
}


TEST(path, append) {
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("foo") / "c:/bar" == "c:/bar");
        ASSERT_TRUE(alkaid::FilePath("foo") / "c:" == "c:");
        ASSERT_TRUE(alkaid::FilePath("c:") / "" == "c:");
        ASSERT_TRUE(alkaid::FilePath("c:foo") / "/bar" == "c:/bar");
        ASSERT_TRUE(alkaid::FilePath("c:foo") / "c:bar" == "c:foo/bar");
#else
    ASSERT_TRUE(alkaid::FilePath("foo") / "" == "foo/");
    ASSERT_TRUE(alkaid::FilePath("foo") / "/bar" == "/bar");
    ASSERT_TRUE(alkaid::FilePath("/foo") / "/" == "/");
    if (has_host_root_name_support()) {
        ASSERT_TRUE(alkaid::FilePath("//host/foo") / "/bar" == "/bar");
        ASSERT_TRUE(alkaid::FilePath("//host") / "/" == "//host/");
        ASSERT_TRUE(alkaid::FilePath("//host/foo") / "/" == "/");
    }
#endif
    ASSERT_TRUE(alkaid::FilePath("/foo/bar") / "some///other" == "/foo/bar/some/other");
    alkaid::FilePath p1{"/tmp/test"};
    alkaid::FilePath p2{"foobar.txt"};
    alkaid::FilePath p3 = p1 / p2;
    ASSERT_TRUE("/tmp/test/foobar.txt" == p3);
// TODO: append(first, last)
}

TEST(path, concat) {
    ASSERT_TRUE((alkaid::FilePath("foo") += alkaid::FilePath("bar")) == "foobar");
    ASSERT_TRUE((alkaid::FilePath("foo") += alkaid::FilePath("/bar")) == "foo/bar");

    ASSERT_TRUE((alkaid::FilePath("foo") += std::string("bar")) == "foobar");
    ASSERT_TRUE((alkaid::FilePath("foo") += std::string("/bar")) == "foo/bar");

    ASSERT_TRUE((alkaid::FilePath("foo") += "bar") == "foobar");
    ASSERT_TRUE((alkaid::FilePath("foo") += "/bar") == "foo/bar");
    ASSERT_TRUE((alkaid::FilePath("foo") += L"bar") == "foobar");
    ASSERT_TRUE((alkaid::FilePath("foo") += L"/bar") == "foo/bar");

    ASSERT_TRUE((alkaid::FilePath("foo") += 'b') == "foob");
    ASSERT_TRUE((alkaid::FilePath("foo") += '/') == "foo/");
    ASSERT_TRUE((alkaid::FilePath("foo") += L'b') == "foob");
    ASSERT_TRUE((alkaid::FilePath("foo") += L'/') == "foo/");

    ASSERT_TRUE((alkaid::FilePath("foo") += std::string("bar")) == "foobar");
    ASSERT_TRUE((alkaid::FilePath("foo") += std::string("/bar")) == "foo/bar");

    ASSERT_TRUE((alkaid::FilePath("foo") += std::u16string(u"bar")) == "foobar");
    ASSERT_TRUE((alkaid::FilePath("foo") += std::u16string(u"/bar")) == "foo/bar");

    ASSERT_TRUE((alkaid::FilePath("foo") += std::u32string(U"bar")) == "foobar");
    ASSERT_TRUE((alkaid::FilePath("foo") += std::u32string(U"/bar")) == "foo/bar");

    ASSERT_TRUE(alkaid::FilePath("foo").concat("bar") == "foobar");
    ASSERT_TRUE(alkaid::FilePath("foo").concat("/bar") == "foo/bar");
    ASSERT_TRUE(alkaid::FilePath("foo").concat(L"bar") == "foobar");
    ASSERT_TRUE(alkaid::FilePath("foo").concat(L"/bar") == "foo/bar");
    std::string bar = "bar";
    ASSERT_TRUE(alkaid::FilePath("foo").concat(bar.begin(), bar.end()) == "foobar");
#ifndef USE_STD_FS
    ASSERT_TRUE((alkaid::FilePath("/foo/bar") += "/some///other") == "/foo/bar/some/other");
#endif
// TODO: contat(first, last)
}

TEST(path, modifiers) {
    alkaid::FilePath p = alkaid::FilePath("/foo/bar");
    p.clear();
    ASSERT_TRUE(p == "");

// make_preferred() is a no-op
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("foo\\bar") == "foo/bar");
        ASSERT_TRUE(alkaid::FilePath("foo\\bar").make_preferred() == "foo/bar");
#else
    ASSERT_TRUE(alkaid::FilePath("foo\\bar") == "foo\\bar");
    ASSERT_TRUE(alkaid::FilePath("foo\\bar").make_preferred() == "foo\\bar");
#endif
    ASSERT_TRUE(alkaid::FilePath("foo/bar").make_preferred() == "foo/bar");

    ASSERT_TRUE(alkaid::FilePath("foo/bar").remove_filename() == "foo/");
    ASSERT_TRUE(alkaid::FilePath("foo/").remove_filename() == "foo/");
    ASSERT_TRUE(alkaid::FilePath("/foo").remove_filename() == "/");
    ASSERT_TRUE(alkaid::FilePath("/").remove_filename() == "/");

    ASSERT_TRUE(alkaid::FilePath("/foo").replace_filename("bar") == "/bar");
    ASSERT_TRUE(alkaid::FilePath("/").replace_filename("bar") == "/bar");
    ASSERT_TRUE(alkaid::FilePath("/foo").replace_filename("b//ar") == "/b/ar");

    ASSERT_TRUE(alkaid::FilePath("/foo/bar.txt").replace_extension("odf") == "/foo/bar.odf");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar.txt").replace_extension() == "/foo/bar");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar").replace_extension("odf") == "/foo/bar.odf");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar").replace_extension(".odf") == "/foo/bar.odf");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar.").replace_extension(".odf") == "/foo/bar.odf");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar/").replace_extension("odf") == "/foo/bar/.odf");

    alkaid::FilePath p1 = "foo";
    alkaid::FilePath p2 = "bar";
    p1.swap(p2);
    ASSERT_TRUE(p1 == "bar");
    ASSERT_TRUE(p2 == "foo");
}

TEST(path, native_obs) {
#ifdef ALKAID_OS_WINDOWS
#if defined(IS_WCHAR_PATH) || defined(ALKAID_USE_WCHAR_T)
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").native() == alkaid::FilePath::string_type(L"\u00E4\\\u20AC"));
    // ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").string() == std::string("ä\\€")); // MSVCs returns local DBCS encoding
#else
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").native() == alkaid::FilePath::string_type("\xc3\xa4\\\xe2\x82\xac"));
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").string() == std::string("\xc3\xa4\\\xe2\x82\xac"));
    ASSERT_TRUE(!::strcmp(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").c_str(), "\xc3\xa4\\\xe2\x82\xac"));
    ASSERT_TRUE((std::string)alkaid::u8path("\xc3\xa4\\\xe2\x82\xac") == std::string("\xc3\xa4\\\xe2\x82\xac"));
#endif
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").wstring() == std::wstring(L"\u00E4\\\u20AC"));
#if defined(__cpp_lib_char8_t) && !defined(ALKAID_FILESYSTEM_ENFORCE_CPP17_API)
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").u8string() == std::u8string(u8"\u00E4\\\u20AC"));
#else
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").u8string() == std::string("\xc3\xa4\\\xe2\x82\xac"));
#endif
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").u16string() == std::u16string(u"\u00E4\\\u20AC"));
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").u32string() == std::u32string(U"\U000000E4\\\U000020AC"));
#else
    ASSERT_TRUE(
            alkaid::u8path("\xc3\xa4/\xe2\x82\xac").native() == alkaid::FilePath::string_type("\xc3\xa4/\xe2\x82\xac"));
    ASSERT_TRUE(!::strcmp(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").c_str(), "\xc3\xa4/\xe2\x82\xac"));
    ASSERT_TRUE((std::string) alkaid::u8path("\xc3\xa4/\xe2\x82\xac") == std::string("\xc3\xa4/\xe2\x82\xac"));
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").string() == std::string("\xc3\xa4/\xe2\x82\xac"));
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").wstring() == std::wstring(L"ä/€"));
#if defined(__cpp_lib_char8_t) && !defined(ALKAID_FILESYSTEM_ENFORCE_CPP17_API)
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").u8string() == std::u8string(u8"\xc3\xa4/\xe2\x82\xac"));
#else
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").u8string() == std::string("\xc3\xa4/\xe2\x82\xac"));
#endif
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").u16string() == std::u16string(u"\u00E4/\u20AC"));
    //GTEST("This check might fail on GCC8 (with \"Illegal byte sequence\") due to not detecting the valid unicode codepoint U+1D11E.");
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac\xf0\x9d\x84\x9e").u16string() ==
                std::u16string(u"\u00E4/\u20AC\U0001D11E"));
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").u32string() == std::u32string(U"\U000000E4/\U000020AC"));
#endif
}

TEST(path, generic) {
#ifdef ALKAID_OS_WINDOWS
#ifndef IS_WCHAR_PATH
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").generic_string() == std::string("\xc3\xa4/\xe2\x82\xac"));
#endif
#ifndef USE_STD_FS
    auto t = alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").generic_string<char, std::char_traits<char>, TestAllocator<char>>();
    ASSERT_TRUE(t.c_str() == std::string("\xc3\xa4/\xe2\x82\xac"));
#endif
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").generic_wstring() == std::wstring(L"\U000000E4/\U000020AC"));
#if defined(__cpp_lib_char8_t) && !defined(ALKAID_FILESYSTEM_ENFORCE_CPP17_API)
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").generic_u8string() == std::u8string(u8"\u00E4/\u20AC"));
#else
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").generic_u8string() == std::string("\xc3\xa4/\xe2\x82\xac"));
#endif
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").generic_u16string() == std::u16string(u"\u00E4/\u20AC"));
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4\\\xe2\x82\xac").generic_u32string() == std::u32string(U"\U000000E4/\U000020AC"));
#else
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").generic_string() == std::string("\xc3\xa4/\xe2\x82\xac"));
#ifndef USE_STD_FS
    auto t = alkaid::u8path("\xc3\xa4/\xe2\x82\xac").generic_string < char, std::char_traits<char>, TestAllocator<char>>
    ();
    ASSERT_TRUE(t.c_str() == std::string("\xc3\xa4/\xe2\x82\xac"));
#endif
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").generic_wstring() == std::wstring(L"ä/€"));
#if defined(__cpp_lib_char8_t) && !defined(ALKAID_FILESYSTEM_ENFORCE_CPP17_API)
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").generic_u8string() == std::u8string(u8"\xc3\xa4/\xe2\x82\xac"));
#else
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").generic_u8string() == std::string("\xc3\xa4/\xe2\x82\xac"));
#endif
    ASSERT_TRUE(alkaid::u8path("\xc3\xa4/\xe2\x82\xac").generic_u16string() == std::u16string(u"\u00E4/\u20AC"));
    ASSERT_TRUE(
            alkaid::u8path("\xc3\xa4/\xe2\x82\xac").generic_u32string() == std::u32string(U"\U000000E4/\U000020AC"));
#endif
}

TEST(path, compare) {
    ASSERT_TRUE(alkaid::FilePath("/foo/b").compare("/foo/a") > 0);
    ASSERT_TRUE(alkaid::FilePath("/foo/b").compare("/foo/b") == 0);
    ASSERT_TRUE(alkaid::FilePath("/foo/b").compare("/foo/c") < 0);

    ASSERT_TRUE(alkaid::FilePath("/foo/b").compare(std::string("/foo/a")) > 0);
    ASSERT_TRUE(alkaid::FilePath("/foo/b").compare(std::string("/foo/b")) == 0);
    ASSERT_TRUE(alkaid::FilePath("/foo/b").compare(std::string("/foo/c")) < 0);

    ASSERT_TRUE(alkaid::FilePath("/foo/b").compare(alkaid::FilePath("/foo/a")) > 0);
    ASSERT_TRUE(alkaid::FilePath("/foo/b").compare(alkaid::FilePath("/foo/b")) == 0);
    ASSERT_TRUE(alkaid::FilePath("/foo/b").compare(alkaid::FilePath("/foo/c")) < 0);

#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("c:\\a\\b").compare("C:\\a\\b") == 0);
        ASSERT_TRUE(alkaid::FilePath("c:\\a\\b").compare("d:\\a\\b") != 0);
        ASSERT_TRUE(alkaid::FilePath("c:\\a\\b").compare("C:\\A\\b") != 0);
#endif

#ifdef LWG_2936_BEHAVIOUR
    ASSERT_TRUE(alkaid::FilePath("/a/b/").compare("/a/b/c") < 0);
    ASSERT_TRUE(alkaid::FilePath("/a/b/").compare("a/c") > 0);
#endif // LWG_2936_BEHAVIOUR
}

TEST(path, decompose) {
// root_name()
    ASSERT_TRUE(alkaid::FilePath("").root_name() == "");
    ASSERT_TRUE(alkaid::FilePath(".").root_name() == "");
    ASSERT_TRUE(alkaid::FilePath("..").root_name() == "");
    ASSERT_TRUE(alkaid::FilePath("foo").root_name() == "");
    ASSERT_TRUE(alkaid::FilePath("/").root_name() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo").root_name() == "");
    ASSERT_TRUE(alkaid::FilePath("foo/").root_name() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo/").root_name() == "");
    ASSERT_TRUE(alkaid::FilePath("foo/bar").root_name() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar").root_name() == "");
    ASSERT_TRUE(alkaid::FilePath("///foo/bar").root_name() == "");
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("C:/foo").root_name() == "C:");
        ASSERT_TRUE(alkaid::FilePath("C:\\foo").root_name() == "C:");
        ASSERT_TRUE(alkaid::FilePath("C:foo").root_name() == "C:");
#endif

// root_directory()
    ASSERT_TRUE(alkaid::FilePath("").root_directory() == "");
    ASSERT_TRUE(alkaid::FilePath(".").root_directory() == "");
    ASSERT_TRUE(alkaid::FilePath("..").root_directory() == "");
    ASSERT_TRUE(alkaid::FilePath("foo").root_directory() == "");
    ASSERT_TRUE(alkaid::FilePath("/").root_directory() == "/");
    ASSERT_TRUE(alkaid::FilePath("/foo").root_directory() == "/");
    ASSERT_TRUE(alkaid::FilePath("foo/").root_directory() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo/").root_directory() == "/");
    ASSERT_TRUE(alkaid::FilePath("foo/bar").root_directory() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar").root_directory() == "/");
    ASSERT_TRUE(alkaid::FilePath("///foo/bar").root_directory() == "/");
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("C:/foo").root_directory() == "/");
        ASSERT_TRUE(alkaid::FilePath("C:\\foo").root_directory() == "/");
        ASSERT_TRUE(alkaid::FilePath("C:foo").root_directory() == "");
#endif

// root_path()
    ASSERT_TRUE(alkaid::FilePath("").root_path() == "");
    ASSERT_TRUE(alkaid::FilePath(".").root_path() == "");
    ASSERT_TRUE(alkaid::FilePath("..").root_path() == "");
    ASSERT_TRUE(alkaid::FilePath("foo").root_path() == "");
    ASSERT_TRUE(alkaid::FilePath("/").root_path() == "/");
    ASSERT_TRUE(alkaid::FilePath("/foo").root_path() == "/");
    ASSERT_TRUE(alkaid::FilePath("foo/").root_path() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo/").root_path() == "/");
    ASSERT_TRUE(alkaid::FilePath("foo/bar").root_path() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar").root_path() == "/");
    ASSERT_TRUE(alkaid::FilePath("///foo/bar").root_path() == "/");
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("C:/foo").root_path() == "C:/");
        ASSERT_TRUE(alkaid::FilePath("C:\\foo").root_path() == "C:/");
        ASSERT_TRUE(alkaid::FilePath("C:foo").root_path() == "C:");
#endif

// relative_path()
    ASSERT_TRUE(alkaid::FilePath("").relative_path() == "");
    ASSERT_TRUE(alkaid::FilePath(".").relative_path() == ".");
    ASSERT_TRUE(alkaid::FilePath("..").relative_path() == "..");
    ASSERT_TRUE(alkaid::FilePath("foo").relative_path() == "foo");
    ASSERT_TRUE(alkaid::FilePath("/").relative_path() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo").relative_path() == "foo");
    ASSERT_TRUE(alkaid::FilePath("foo/").relative_path() == "foo/");
    ASSERT_TRUE(alkaid::FilePath("/foo/").relative_path() == "foo/");
    ASSERT_TRUE(alkaid::FilePath("foo/bar").relative_path() == "foo/bar");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar").relative_path() == "foo/bar");
    ASSERT_TRUE(alkaid::FilePath("///foo/bar").relative_path() == "foo/bar");
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("C:/foo").relative_path() == "foo");
        ASSERT_TRUE(alkaid::FilePath("C:\\foo").relative_path() == "foo");
        ASSERT_TRUE(alkaid::FilePath("C:foo").relative_path() == "foo");
#endif

// parent_path()
    ASSERT_TRUE(alkaid::FilePath("").parent_path() == "");
    ASSERT_TRUE(alkaid::FilePath(".").parent_path() == "");
    ASSERT_TRUE(alkaid::FilePath("..").parent_path() == "");  // unintuitive but as defined in the standard
    ASSERT_TRUE(alkaid::FilePath("foo").parent_path() == "");
    ASSERT_TRUE(alkaid::FilePath("/").parent_path() == "/");
    ASSERT_TRUE(alkaid::FilePath("/foo").parent_path() == "/");
    ASSERT_TRUE(alkaid::FilePath("foo/").parent_path() == "foo");
    ASSERT_TRUE(alkaid::FilePath("/foo/").parent_path() == "/foo");
    ASSERT_TRUE(alkaid::FilePath("foo/bar").parent_path() == "foo");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar").parent_path() == "/foo");
    ASSERT_TRUE(alkaid::FilePath("///foo/bar").parent_path() == "/foo");
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("C:/foo").parent_path() == "C:/");
        ASSERT_TRUE(alkaid::FilePath("C:\\foo").parent_path() == "C:/");
        ASSERT_TRUE(alkaid::FilePath("C:foo").parent_path() == "C:");
#endif

// filename()
    ASSERT_TRUE(alkaid::FilePath("").filename() == "");
    ASSERT_TRUE(alkaid::FilePath(".").filename() == ".");
    ASSERT_TRUE(alkaid::FilePath("..").filename() == "..");
    ASSERT_TRUE(alkaid::FilePath("foo").filename() == "foo");
    ASSERT_TRUE(alkaid::FilePath("/").filename() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo").filename() == "foo");
    ASSERT_TRUE(alkaid::FilePath("foo/").filename() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo/").filename() == "");
    ASSERT_TRUE(alkaid::FilePath("foo/bar").filename() == "bar");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar").filename() == "bar");
    ASSERT_TRUE(alkaid::FilePath("///foo/bar").filename() == "bar");
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("C:/foo").filename() == "foo");
        ASSERT_TRUE(alkaid::FilePath("C:\\foo").filename() == "foo");
        ASSERT_TRUE(alkaid::FilePath("C:foo").filename() == "foo");
        ASSERT_TRUE(alkaid::FilePath("t:est.txt").filename() == "est.txt");
#else
    ASSERT_TRUE(alkaid::FilePath("t:est.txt").filename() == "t:est.txt");
#endif

// stem()
    ASSERT_TRUE(alkaid::FilePath("/foo/bar.txt").stem() == "bar");
    {
        alkaid::FilePath p = "foo.bar.baz.tar";
        ASSERT_TRUE(p.extension() == ".tar");
        p = p.stem();
        ASSERT_TRUE(p.extension() == ".baz");
        p = p.stem();
        ASSERT_TRUE(p.extension() == ".bar");
        p = p.stem();
        ASSERT_TRUE(p == "foo");
    }
    ASSERT_TRUE(alkaid::FilePath("/foo/.profile").stem() == ".profile");
    ASSERT_TRUE(alkaid::FilePath(".bar").stem() == ".bar");
    ASSERT_TRUE(alkaid::FilePath("..bar").stem() == ".");
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("t:est.txt").stem() == "est");
#else
    ASSERT_TRUE(alkaid::FilePath("t:est.txt").stem() == "t:est");
#endif
    ASSERT_TRUE(alkaid::FilePath("/foo/.").stem() == ".");
    ASSERT_TRUE(alkaid::FilePath("/foo/..").stem() == "..");

// extension()
    ASSERT_TRUE(alkaid::FilePath("/foo/bar.txt").extension() == ".txt");
    ASSERT_TRUE(alkaid::FilePath("/foo/bar").extension() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo/.profile").extension() == "");
    ASSERT_TRUE(alkaid::FilePath(".bar").extension() == "");
    ASSERT_TRUE(alkaid::FilePath("..bar").extension() == ".bar");
    ASSERT_TRUE(alkaid::FilePath("t:est.txt").extension() == ".txt");
    ASSERT_TRUE(alkaid::FilePath("/foo/.").extension() == "");
    ASSERT_TRUE(alkaid::FilePath("/foo/..").extension() == "");

    if (has_host_root_name_support()) {
// //host-based root-names
        ASSERT_TRUE(alkaid::FilePath("//host").root_name() == "//host");
        ASSERT_TRUE(alkaid::FilePath("//host/foo").root_name() == "//host");
        ASSERT_TRUE(alkaid::FilePath("//host").root_directory() == "");
        ASSERT_TRUE(alkaid::FilePath("//host/foo").root_directory() == "/");
        ASSERT_TRUE(alkaid::FilePath("//host").root_path() == "//host");
        ASSERT_TRUE(alkaid::FilePath("//host/foo").root_path() == "//host/");
        ASSERT_TRUE(alkaid::FilePath("//host").relative_path() == "");
        ASSERT_TRUE(alkaid::FilePath("//host/foo").relative_path() == "foo");
        ASSERT_TRUE(alkaid::FilePath("//host").parent_path() == "//host");
        ASSERT_TRUE(alkaid::FilePath("//host/foo").parent_path() == "//host/");
        ASSERT_TRUE(alkaid::FilePath("//host").filename() == "");
        ASSERT_TRUE(alkaid::FilePath("//host/foo").filename() == "foo");
    }
}


TEST(path, query) {
// empty
    ASSERT_TRUE(alkaid::FilePath("").empty());
    ASSERT_TRUE(!alkaid::FilePath("foo").empty());

// has_root_path()
    ASSERT_TRUE(!alkaid::FilePath("foo").has_root_path());
    ASSERT_TRUE(!alkaid::FilePath("foo/bar").has_root_path());
    ASSERT_TRUE(alkaid::FilePath("/foo").has_root_path());
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("C:foo").has_root_path());
        ASSERT_TRUE(alkaid::FilePath("C:/foo").has_root_path());
#endif

// has_root_name()
    ASSERT_TRUE(!alkaid::FilePath("foo").has_root_name());
    ASSERT_TRUE(!alkaid::FilePath("foo/bar").has_root_name());
    ASSERT_TRUE(!alkaid::FilePath("/foo").has_root_name());
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("C:foo").has_root_name());
        ASSERT_TRUE(alkaid::FilePath("C:/foo").has_root_name());
#endif

// has_root_directory()
    ASSERT_TRUE(!alkaid::FilePath("foo").has_root_directory());
    ASSERT_TRUE(!alkaid::FilePath("foo/bar").has_root_directory());
    ASSERT_TRUE(alkaid::FilePath("/foo").has_root_directory());
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(!alkaid::FilePath("C:foo").has_root_directory());
        ASSERT_TRUE(alkaid::FilePath("C:/foo").has_root_directory());
#endif

// has_relative_path()
    ASSERT_TRUE(!alkaid::FilePath("").has_relative_path());
    ASSERT_TRUE(!alkaid::FilePath("/").has_relative_path());
    ASSERT_TRUE(alkaid::FilePath("/foo").has_relative_path());

// has_parent_path()
    ASSERT_TRUE(!alkaid::FilePath("").has_parent_path());
    ASSERT_TRUE(!alkaid::FilePath(".").has_parent_path());
    ASSERT_TRUE(!alkaid::FilePath("..").has_parent_path());  // unintuitive but as defined in the standard
    ASSERT_TRUE(!alkaid::FilePath("foo").has_parent_path());
    ASSERT_TRUE(alkaid::FilePath("/").has_parent_path());
    ASSERT_TRUE(alkaid::FilePath("/foo").has_parent_path());
    ASSERT_TRUE(alkaid::FilePath("foo/").has_parent_path());
    ASSERT_TRUE(alkaid::FilePath("/foo/").has_parent_path());

// has_filename()
    ASSERT_TRUE(alkaid::FilePath("foo").has_filename());
    ASSERT_TRUE(alkaid::FilePath("foo/bar").has_filename());
    ASSERT_TRUE(!alkaid::FilePath("/foo/bar/").has_filename());

// has_stem()
    ASSERT_TRUE(alkaid::FilePath("foo").has_stem());
    ASSERT_TRUE(alkaid::FilePath("foo.bar").has_stem());
    ASSERT_TRUE(alkaid::FilePath(".profile").has_stem());
    ASSERT_TRUE(!alkaid::FilePath("/foo/").has_stem());

// has_extension()
    ASSERT_TRUE(!alkaid::FilePath("foo").has_extension());
    ASSERT_TRUE(alkaid::FilePath("foo.bar").has_extension());
    ASSERT_TRUE(!alkaid::FilePath(".profile").has_extension());

// is_absolute()
    ASSERT_TRUE(!alkaid::FilePath("foo/bar").is_absolute());
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(!alkaid::FilePath("/foo").is_absolute());
        ASSERT_TRUE(!alkaid::FilePath("c:foo").is_absolute());
        ASSERT_TRUE(alkaid::FilePath("c:/foo").is_absolute());
#else
    ASSERT_TRUE(alkaid::FilePath("/foo").is_absolute());
#endif

// is_relative()
    ASSERT_TRUE(alkaid::FilePath("foo/bar").is_relative());
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("/foo").is_relative());
        ASSERT_TRUE(alkaid::FilePath("c:foo").is_relative());
        ASSERT_TRUE(!alkaid::FilePath("c:/foo").is_relative());
#else
    ASSERT_TRUE(!alkaid::FilePath("/foo").is_relative());
#endif

    if (has_host_root_name_support()) {
        ASSERT_TRUE(alkaid::FilePath("//host").has_root_name());
        ASSERT_TRUE(alkaid::FilePath("//host/foo").has_root_name());
        ASSERT_TRUE(alkaid::FilePath("//host").has_root_path());
        ASSERT_TRUE(alkaid::FilePath("//host/foo").has_root_path());
        ASSERT_TRUE(!alkaid::FilePath("//host").has_root_directory());
        ASSERT_TRUE(alkaid::FilePath("//host/foo").has_root_directory());
        ASSERT_TRUE(!alkaid::FilePath("//host").has_relative_path());
        ASSERT_TRUE(alkaid::FilePath("//host/foo").has_relative_path());
        ASSERT_TRUE(alkaid::FilePath("//host/foo").is_absolute());
        ASSERT_TRUE(!alkaid::FilePath("//host/foo").is_relative());
    }
}

TEST(path, gen) {
// lexically_normal()
    ASSERT_TRUE(alkaid::FilePath("foo/./bar/..").lexically_normal() == "foo/");
    ASSERT_TRUE(alkaid::FilePath("foo/.///bar/../").lexically_normal() == "foo/");
    ASSERT_TRUE(alkaid::FilePath("/foo/../..").lexically_normal() == "/");
    ASSERT_TRUE(alkaid::FilePath("foo/..").lexically_normal() == ".");
    ASSERT_TRUE(alkaid::FilePath("ab/cd/ef/../../qw").lexically_normal() == "ab/qw");
    ASSERT_TRUE(alkaid::FilePath("a/b/../../../c").lexically_normal() == "../c");
    ASSERT_TRUE(alkaid::FilePath("../").lexically_normal() == "..");
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("\\/\\///\\/").lexically_normal() == "/");
        ASSERT_TRUE(alkaid::FilePath("a/b/..\\//..///\\/../c\\\\/").lexically_normal() == "../c/");
        ASSERT_TRUE(alkaid::FilePath("..a/b/..\\//..///\\/../c\\\\/").lexically_normal() == "../c/");
        ASSERT_TRUE(alkaid::FilePath("..\\").lexically_normal() == "..");
#endif

// lexically_relative()
    ASSERT_TRUE(alkaid::FilePath("/a/d").lexically_relative("/a/b/c") == "../../d");
    ASSERT_TRUE(alkaid::FilePath("/a/b/c").lexically_relative("/a/d") == "../b/c");
    ASSERT_TRUE(alkaid::FilePath("/a/b/c").lexically_relative("/a/b/c/d/..") == ".");
    ASSERT_TRUE(alkaid::FilePath("/a/b/c/").lexically_relative("/a/b/c/d/..") == ".");
    ASSERT_TRUE(alkaid::FilePath("").lexically_relative("/a/..") == "");
    ASSERT_TRUE(alkaid::FilePath("").lexically_relative("a/..") == ".");
    ASSERT_TRUE(alkaid::FilePath("a/b/c").lexically_relative("a") == "b/c");
    ASSERT_TRUE(alkaid::FilePath("a/b/c").lexically_relative("a/b/c/x/y") == "../..");
    ASSERT_TRUE(alkaid::FilePath("a/b/c").lexically_relative("a/b/c") == ".");
    ASSERT_TRUE(alkaid::FilePath("a/b").lexically_relative("c/d") == "../../a/b");
    ASSERT_TRUE(alkaid::FilePath("a/b").lexically_relative("a/") == "b");
    if (has_host_root_name_support()) {
        ASSERT_TRUE(alkaid::FilePath("//host1/foo").lexically_relative("//host2.bar") == "");
    }
#ifdef ALKAID_OS_WINDOWS
        ASSERT_TRUE(alkaid::FilePath("c:/foo").lexically_relative("/bar") == "");
            ASSERT_TRUE(alkaid::FilePath("c:foo").lexically_relative("c:/bar") == "");
            ASSERT_TRUE(alkaid::FilePath("foo").lexically_relative("/bar") == "");
            ASSERT_TRUE(alkaid::FilePath("c:/foo/bar.txt").lexically_relative("c:/foo/") == "bar.txt");
            ASSERT_TRUE(alkaid::FilePath("c:/foo/bar.txt").lexically_relative("C:/foo/") == "bar.txt");
#else
    ASSERT_TRUE(alkaid::FilePath("/foo").lexically_relative("bar") == "");
    ASSERT_TRUE(alkaid::FilePath("foo").lexically_relative("/bar") == "");
#endif

// lexically_proximate()
    ASSERT_TRUE(alkaid::FilePath("/a/d").lexically_proximate("/a/b/c") == "../../d");
    if (has_host_root_name_support()) {
        ASSERT_TRUE(alkaid::FilePath("//host1/a/d").lexically_proximate("//host2/a/b/c") == "//host1/a/d");
    }
    ASSERT_TRUE(alkaid::FilePath("a/d").lexically_proximate("/a/b/c") == "a/d");
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE(alkaid::FilePath("c:/a/d").lexically_proximate("c:/a/b/c") == "../../d");
        ASSERT_TRUE(alkaid::FilePath("c:/a/d").lexically_proximate("d:/a/b/c") == "c:/a/d");
        ASSERT_TRUE(alkaid::FilePath("c:/foo").lexically_proximate("/bar") == "c:/foo");
        ASSERT_TRUE(alkaid::FilePath("c:foo").lexically_proximate("c:/bar") == "c:foo");
        ASSERT_TRUE(alkaid::FilePath("foo").lexically_proximate("/bar") == "foo");
#else
    ASSERT_TRUE(alkaid::FilePath("/foo").lexically_proximate("bar") == "/foo");
    ASSERT_TRUE(alkaid::FilePath("foo").lexically_proximate("/bar") == "foo");
#endif
}

static std::string iterateResult(const alkaid::FilePath &path) {
    std::ostringstream result;
    for (alkaid::FilePath::const_iterator i = path.begin(); i != path.end(); ++i) {
        if (i != path.begin()) {
            result << ",";
        }
        result << i->generic_string();
    }
    return result.str();
}

static std::string reverseIterateResult(const alkaid::FilePath &path) {
    std::ostringstream result;
    alkaid::FilePath::const_iterator iter = path.end();
    bool first = true;
    if (iter != path.begin()) {
        do {
            --iter;
            if (!first) {
                result << ",";
            }
            first = false;
            result << iter->generic_string();
        } while (iter != path.begin());
    }
    return result.str();
}

TEST(path, iterators) {
    ASSERT_TRUE(iterateResult(alkaid::FilePath()).empty());
    ASSERT_TRUE("." == iterateResult(alkaid::FilePath(".")));
    ASSERT_TRUE(".." == iterateResult(alkaid::FilePath("..")));
    ASSERT_TRUE("foo" == iterateResult(alkaid::FilePath("foo")));
    ASSERT_TRUE("/" == iterateResult(alkaid::FilePath("/")));
    ASSERT_TRUE("/,foo" == iterateResult(alkaid::FilePath("/foo")));
    ASSERT_TRUE("foo," == iterateResult(alkaid::FilePath("foo/")));
    ASSERT_TRUE("/,foo," == iterateResult(alkaid::FilePath("/foo/")));
    ASSERT_TRUE("foo,bar" == iterateResult(alkaid::FilePath("foo/bar")));
    ASSERT_TRUE("/,foo,bar" == iterateResult(alkaid::FilePath("/foo/bar")));
#ifndef USE_STD_FS
// ghc::filesystem enforces redundant slashes to be reduced to one
    ASSERT_TRUE("/,foo,bar" == iterateResult(alkaid::FilePath("///foo/bar")));
#else
    // typically std::filesystem keeps them
        ASSERT_TRUE("///,foo,bar" == iterateResult(alkaid::FilePath("///foo/bar")));
#endif
    ASSERT_TRUE("/,foo,bar," == iterateResult(alkaid::FilePath("/foo/bar///")));
    ASSERT_TRUE("foo,.,bar,..," == iterateResult(alkaid::FilePath("foo/.///bar/../")));
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE("C:,/,foo" == iterateResult(alkaid::FilePath("C:/foo")));
#endif

    ASSERT_TRUE(reverseIterateResult(alkaid::FilePath()).empty());
    ASSERT_TRUE("." == reverseIterateResult(alkaid::FilePath(".")));
    ASSERT_TRUE(".." == reverseIterateResult(alkaid::FilePath("..")));
    ASSERT_TRUE("foo" == reverseIterateResult(alkaid::FilePath("foo")));
    ASSERT_TRUE("/" == reverseIterateResult(alkaid::FilePath("/")));
    ASSERT_TRUE("foo,/" == reverseIterateResult(alkaid::FilePath("/foo")));
    ASSERT_TRUE(",foo" == reverseIterateResult(alkaid::FilePath("foo/")));
    ASSERT_TRUE(",foo,/" == reverseIterateResult(alkaid::FilePath("/foo/")));
    ASSERT_TRUE("bar,foo" == reverseIterateResult(alkaid::FilePath("foo/bar")));
    ASSERT_TRUE("bar,foo,/" == reverseIterateResult(alkaid::FilePath("/foo/bar")));
#ifndef USE_STD_FS
// ghc::filesystem enforces redundant slashes to be reduced to one
    ASSERT_TRUE("bar,foo,/" == reverseIterateResult(alkaid::FilePath("///foo/bar")));
#else
    // typically std::filesystem keeps them
        ASSERT_TRUE("bar,foo,///" == reverseIterateResult(alkaid::FilePath("///foo/bar")));
#endif
    ASSERT_TRUE(",bar,foo,/" == reverseIterateResult(alkaid::FilePath("/foo/bar///")));
    ASSERT_TRUE(",..,bar,.,foo" == reverseIterateResult(alkaid::FilePath("foo/.///bar/../")));
#ifdef ALKAID_OS_WINDOWS
    ASSERT_TRUE("foo,/,C:" == reverseIterateResult(alkaid::FilePath("C:/foo")));
        ASSERT_TRUE("foo,C:" == reverseIterateResult(alkaid::FilePath("C:foo")));
#endif
    {
        alkaid::FilePath p1 = "/foo/bar/test.txt";
        alkaid::FilePath p2;
        for (auto pe: p1) {
            p2 /= pe;
        }
        ASSERT_TRUE(p1 == p2);
        ASSERT_TRUE("bar" == *(--alkaid::FilePath("/foo/bar").end()));
        auto p = alkaid::FilePath("/foo/bar");
        auto pi = p.end();
        pi--;
        ASSERT_TRUE("bar" == *pi);
    }

    if (has_host_root_name_support()) {
        ASSERT_TRUE("foo" == *(--alkaid::FilePath("//host/foo").end()));
        auto p = alkaid::FilePath("//host/foo");
        auto pi = p.end();
        pi--;
        ASSERT_TRUE("foo" == *pi);
        ASSERT_TRUE("//host" == iterateResult(alkaid::FilePath("//host")));
        ASSERT_TRUE("//host,/,foo" == iterateResult(alkaid::FilePath("//host/foo")));
        ASSERT_TRUE("//host" == reverseIterateResult(alkaid::FilePath("//host")));
        ASSERT_TRUE("foo,/,//host" == reverseIterateResult(alkaid::FilePath("//host/foo")));
        {
            alkaid::FilePath p1 = "//host/foo/bar/test.txt";
            alkaid::FilePath p2;
            for (auto pe: p1) {
                p2 /= pe;
            }
            ASSERT_TRUE(p1 == p2);
        }
    }
}

TEST(path, nonmember) {
    alkaid::FilePath p1("foo/bar");
    alkaid::FilePath p2("some/other");
    alkaid::swap(p1, p2);
    ASSERT_TRUE(p1 == "some/other");
    ASSERT_TRUE(p2 == "foo/bar");
    ASSERT_TRUE(hash_value(p1));
    ASSERT_TRUE(p2 < p1);
    ASSERT_TRUE(p2 <= p1);
    ASSERT_TRUE(p1 <= p1);
    ASSERT_TRUE(!(p1 < p2));
    ASSERT_TRUE(!(p1 <= p2));
    ASSERT_TRUE(p1 > p2);
    ASSERT_TRUE(p1 >= p2);
    ASSERT_TRUE(p1 >= p1);
    ASSERT_TRUE(!(p2 > p1));
    ASSERT_TRUE(!(p2 >= p1));
    ASSERT_TRUE(p1 != p2);
    ASSERT_TRUE(p1 / p2 == "some/other/foo/bar");
}

TEST(path, inserter) {
    {
        std::ostringstream os;
        os << alkaid::FilePath("/root/foo bar");
#ifdef ALKAID_OS_WINDOWS
        ASSERT_TRUE(os.str() == "\"\\\\root\\\\foo bar\"");
#else
        ASSERT_TRUE(os.str() == "\"/root/foo bar\"");
#endif
    }
    {
        std::ostringstream os;
        os << alkaid::FilePath("/root/foo\"bar");
#ifdef ALKAID_OS_WINDOWS
        ASSERT_TRUE(os.str() == "\"\\\\root\\\\foo\\\"bar\"");
#else
        ASSERT_TRUE(os.str() == "\"/root/foo\\\"bar\"");
#endif
    }

    {
        std::istringstream is("\"/root/foo bar\"");
        alkaid::FilePath p;
        is >>
           p;
        ASSERT_TRUE(p == alkaid::FilePath("/root/foo bar"));
        ASSERT_TRUE((is.flags() & std::ios_base::skipws) == std::ios_base::skipws);
    }
    {
        std::istringstream is("\"/root/foo bar\"");
        is >>
           std::noskipws;
        alkaid::FilePath p;
        is >>
           p;
        ASSERT_TRUE(p == alkaid::FilePath("/root/foo bar"));
        ASSERT_TRUE((is.flags() & std::ios_base::skipws) != std::ios_base::skipws);
    }
    {
        std::istringstream is("\"/root/foo\\\"bar\"");
        alkaid::FilePath p;
        is >>
           p;
        ASSERT_TRUE(p == alkaid::FilePath("/root/foo\"bar"));
    }
    {
        std::istringstream is("/root/foo");
        alkaid::FilePath p;
        is >>
           p;
        ASSERT_TRUE(p == alkaid::FilePath("/root/foo"));
    }
}

TEST(path, factory) {
    ASSERT_TRUE(alkaid::u8path("foo/bar") == alkaid::FilePath("foo/bar"));
    ASSERT_TRUE(alkaid::u8path("foo/bar") == alkaid::FilePath("foo/bar"));
    std::string str("/foo/bar/test.txt");
    ASSERT_TRUE(alkaid::u8path(str.begin(), str.end()) == str);
}