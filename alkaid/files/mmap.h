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

#pragma once

#include <alkaid/files/internal/page.h>
#include <alkaid/files/internal/string_util.h>
#include <iterator>
#include <string>
#include <system_error>
#include <cstdint>

#ifndef _WIN32

# include <unistd.h>
# include <fcntl.h>
# include <sys/mman.h>
# include <sys/stat.h>

#endif

#ifdef _WIN32
# ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
# endif // WIN32_LEAN_AND_MEAN
# include <windows.h>
#else // ifdef _WIN32
# define INVALID_HANDLE_VALUE -1
#endif // ifdef _WIN32

namespace alkaid {

// This value may be provided as the `length` parameter to the constructor or
// `map`, in which case a memory mapping of the entire file is created.
    enum {
        map_entire_file = 0
    };

#ifdef _WIN32
    using file_handle_type = HANDLE;
#else
    using file_handle_type = int;
#endif

    // This value represents an invalid file handle type. This can be used to
    // determine whether `basic_mmap::file_handle` is valid, for example.
    const static file_handle_type invalid_handle = INVALID_HANDLE_VALUE;

    template<access_mode AccessMode, typename ByteT>
    struct basic_mmap {
        using value_type = ByteT;
        using size_type = size_t;
        using reference = value_type &;
        using const_reference = const value_type &;
        using pointer = value_type *;
        using const_pointer = const value_type *;
        using difference_type = std::ptrdiff_t;
        using iterator = pointer;
        using const_iterator = const_pointer;
        using reverse_iterator = std::reverse_iterator<iterator>;
        using const_reverse_iterator = std::reverse_iterator<const_iterator>;
        using iterator_category = std::random_access_iterator_tag;
        using handle_type = file_handle_type;

        static_assert(sizeof(ByteT) == sizeof(char), "ByteT must be the same size as char.");

    private:
        // Points to the first requested byte, and not to the actual start of the mapping.
        pointer data_ = nullptr;

        // Length--in bytes--requested by user (which may not be the length of the
        // full mapping) and the length of the full mapping.
        size_type length_ = 0;
        size_type mapped_length_ = 0;

        // Letting user map a file using both an existing file handle and a path
        // introcudes some complexity (see `is_handle_internal_`).
        // On POSIX, we only need a file handle to create a mapping, while on
        // Windows systems the file handle is necessary to retrieve a file mapping
        // handle, but any subsequent operations on the mapped region must be done
        // through the latter.
        handle_type file_handle_ = INVALID_HANDLE_VALUE;
#ifdef _WIN32
        handle_type file_mapping_handle_ = INVALID_HANDLE_VALUE;
#endif

        // Letting user map a file using both an existing file handle and a path
        // introcudes some complexity in that we must not close the file handle if
        // user provided it, but we must close it if we obtained it using the
        // provided path. For this reason, this flag is used to determine when to
        // close `file_handle_`.
        bool is_handle_internal_;

    public:
        /**
         * The default constructed mmap object is in a non-mapped state, that is,
         * any operation that attempts to access nonexistent underlying data will
         * result in undefined behaviour/segmentation faults.
         */
        basic_mmap() = default;

#ifdef __cpp_exceptions

        /**
         * The same as invoking the `map` function, except any error that may occur
         * while establishing the mapping is wrapped in a `std::system_error` and is
         * thrown.
         */
        template<typename String>
        basic_mmap(const String &path, const size_type offset = 0, const size_type length = map_entire_file) {
            std::error_code error;
            map(path, offset, length, error);
            if (error) { throw std::system_error(error); }
        }

        /**
         * The same as invoking the `map` function, except any error that may occur
         * while establishing the mapping is wrapped in a `std::system_error` and is
         * thrown.
         */
        basic_mmap(const handle_type handle, const size_type offset = 0, const size_type length = map_entire_file) {
            std::error_code error;
            map(handle, offset, length, error);
            if (error) { throw std::system_error(error); }
        }

#endif // __cpp_exceptions

        /**
         * `basic_mmap` has single-ownership semantics, so transferring ownership
         * may only be accomplished by moving the object.
         */
        basic_mmap(const basic_mmap &) = delete;

        basic_mmap(basic_mmap &&);

        basic_mmap &operator=(const basic_mmap &) = delete;

        basic_mmap &operator=(basic_mmap &&);

        /**
         * If this is a read-write mapping, the destructor invokes sync. Regardless
         * of the access mode, unmap is invoked as a final step.
         */
        ~basic_mmap();

        /**
         * On UNIX systems 'file_handle' and 'mapping_handle' are the same. On Windows,
         * however, a mapped region of a file gets its own handle, which is returned by
         * 'mapping_handle'.
         */
        handle_type file_handle() const noexcept { return file_handle_; }

        handle_type mapping_handle() const noexcept;

        /** Returns whether a valid memory mapping has been created. */
        bool is_open() const noexcept { return file_handle_ != invalid_handle; }

        /**
         * Returns true if no mapping was established, that is, conceptually the
         * same as though the length that was mapped was 0. This function is
         * provided so that this class has Container semantics.
         */
        bool empty() const noexcept { return length() == 0; }

        /** Returns true if a mapping was established. */
        bool is_mapped() const noexcept;

        /**
         * `size` and `length` both return the logical length, i.e. the number of bytes
         * user requested to be mapped, while `mapped_length` returns the actual number of
         * bytes that were mapped which is a multiple of the underlying operating system's
         * page allocation granularity.
         */
        size_type size() const noexcept { return length(); }

        size_type length() const noexcept { return length_; }

        size_type mapped_length() const noexcept { return mapped_length_; }

        /** Returns the offset relative to the start of the mapping. */
        size_type mapping_offset() const noexcept {
            return mapped_length_ - length_;
        }

        /**
         * Returns a pointer to the first requested byte, or `nullptr` if no memory mapping
         * exists.
         */
        template<
                access_mode A = AccessMode,
                typename = typename std::enable_if<A == access_mode::write>::type
        >
        pointer data() noexcept { return data_; }

        const_pointer data() const noexcept { return data_; }

        /**
         * Returns an iterator to the first requested byte, if a valid memory mapping
         * exists, otherwise this function call is undefined behaviour.
         */
        template<
                access_mode A = AccessMode,
                typename = typename std::enable_if<A == access_mode::write>::type
        >
        iterator begin() noexcept { return data(); }

        const_iterator begin() const noexcept { return data(); }

        const_iterator cbegin() const noexcept { return data(); }

        /**
         * Returns an iterator one past the last requested byte, if a valid memory mapping
         * exists, otherwise this function call is undefined behaviour.
         */
        template<
                access_mode A = AccessMode,
                typename = typename std::enable_if<A == access_mode::write>::type
        >
        iterator end() noexcept { return data() + length(); }

        const_iterator end() const noexcept { return data() + length(); }

        const_iterator cend() const noexcept { return data() + length(); }

        /**
         * Returns a reverse iterator to the last memory mapped byte, if a valid
         * memory mapping exists, otherwise this function call is undefined
         * behaviour.
         */
        template<
                access_mode A = AccessMode,
                typename = typename std::enable_if<A == access_mode::write>::type
        >
        reverse_iterator rbegin() noexcept { return reverse_iterator(end()); }

        const_reverse_iterator rbegin() const noexcept { return const_reverse_iterator(end()); }

        const_reverse_iterator crbegin() const noexcept { return const_reverse_iterator(end()); }

        /**
         * Returns a reverse iterator past the first mapped byte, if a valid memory
         * mapping exists, otherwise this function call is undefined behaviour.
         */
        template<
                access_mode A = AccessMode,
                typename = typename std::enable_if<A == access_mode::write>::type
        >
        reverse_iterator rend() noexcept { return reverse_iterator(begin()); }

        const_reverse_iterator rend() const noexcept { return const_reverse_iterator(begin()); }

        const_reverse_iterator crend() const noexcept { return const_reverse_iterator(begin()); }

        /**
         * Returns a reference to the `i`th byte from the first requested byte (as returned
         * by `data`). If this is invoked when no valid memory mapping has been created
         * prior to this call, undefined behaviour ensues.
         */
        reference operator[](const size_type i) noexcept { return data_[i]; }

        const_reference operator[](const size_type i) const noexcept { return data_[i]; }

        /**
         * Establishes a memory mapping with AccessMode. If the mapping is unsuccesful, the
         * reason is reported via `error` and the object remains in a state as if this
         * function hadn't been called.
         *
         * `path`, which must be a path to an existing file, is used to retrieve a file
         * handle (which is closed when the object destructs or `unmap` is called), which is
         * then used to memory map the requested region. Upon failure, `error` is set to
         * indicate the reason and the object remains in an unmapped state.
         *
         * `offset` is the number of bytes, relative to the start of the file, where the
         * mapping should begin. When specifying it, there is no need to worry about
         * providing a value that is aligned with the operating system's page allocation
         * granularity. This is adjusted by the implementation such that the first requested
         * byte (as returned by `data` or `begin`), so long as `offset` is valid, will be at
         * `offset` from the start of the file.
         *
         * `length` is the number of bytes to map. It may be `map_entire_file`, in which
         * case a mapping of the entire file is created.
         */
        template<typename String>
        void map(const String &path, const size_type offset,
                 const size_type length, std::error_code &error);

        /**
         * Establishes a memory mapping with AccessMode. If the mapping is unsuccesful, the
         * reason is reported via `error` and the object remains in a state as if this
         * function hadn't been called.
         *
         * `path`, which must be a path to an existing file, is used to retrieve a file
         * handle (which is closed when the object destructs or `unmap` is called), which is
         * then used to memory map the requested region. Upon failure, `error` is set to
         * indicate the reason and the object remains in an unmapped state.
         *
         * The entire file is mapped.
         */
        template<typename String>
        void map(const String &path, std::error_code &error) {
            map(path, 0, map_entire_file, error);
        }

        /**
         * Establishes a memory mapping with AccessMode. If the mapping is
         * unsuccesful, the reason is reported via `error` and the object remains in
         * a state as if this function hadn't been called.
         *
         * `handle`, which must be a valid file handle, which is used to memory map the
         * requested region. Upon failure, `error` is set to indicate the reason and the
         * object remains in an unmapped state.
         *
         * `offset` is the number of bytes, relative to the start of the file, where the
         * mapping should begin. When specifying it, there is no need to worry about
         * providing a value that is aligned with the operating system's page allocation
         * granularity. This is adjusted by the implementation such that the first requested
         * byte (as returned by `data` or `begin`), so long as `offset` is valid, will be at
         * `offset` from the start of the file.
         *
         * `length` is the number of bytes to map. It may be `map_entire_file`, in which
         * case a mapping of the entire file is created.
         */
        void map(const handle_type handle, const size_type offset,
                 const size_type length, std::error_code &error);

        /**
         * Establishes a memory mapping with AccessMode. If the mapping is
         * unsuccesful, the reason is reported via `error` and the object remains in
         * a state as if this function hadn't been called.
         *
         * `handle`, which must be a valid file handle, which is used to memory map the
         * requested region. Upon failure, `error` is set to indicate the reason and the
         * object remains in an unmapped state.
         *
         * The entire file is mapped.
         */
        void map(const handle_type handle, std::error_code &error) {
            map(handle, 0, map_entire_file, error);
        }

        /**
         * If a valid memory mapping has been created prior to this call, this call
         * instructs the kernel to unmap the memory region and disassociate this object
         * from the file.
         *
         * The file handle associated with the file that is mapped is only closed if the
         * mapping was created using a file path. If, on the other hand, an existing
         * file handle was used to create the mapping, the file handle is not closed.
         */
        void unmap();

        void swap(basic_mmap &other);

        /** Flushes the memory mapped page to disk. Errors are reported via `error`. */
        template<access_mode A = AccessMode>
        typename std::enable_if<A == access_mode::write, void>::type
        sync(std::error_code &error);

        /**
         * All operators compare the address of the first byte and size of the two mapped
         * regions.
         */

    private:
        template<
                access_mode A = AccessMode,
                typename = typename std::enable_if<A == access_mode::write>::type
        >
        pointer get_mapping_start() noexcept {
            return !data() ? nullptr : data() - mapping_offset();
        }

        const_pointer get_mapping_start() const noexcept {
            return !data() ? nullptr : data() - mapping_offset();
        }

        /**
         * The destructor syncs changes to disk if `AccessMode` is `write`, but not
         * if it's `read`, but since the destructor cannot be templated, we need to
         * do SFINAE in a dedicated function, where one syncs and the other is a noop.
         */
        template<access_mode A = AccessMode>
        typename std::enable_if<A == access_mode::write, void>::type
        conditional_sync();

        template<access_mode A = AccessMode>
        typename std::enable_if<A == access_mode::read, void>::type conditional_sync();
    };

    template<access_mode AccessMode, typename ByteT>
    bool operator==(const basic_mmap<AccessMode, ByteT> &a,
                    const basic_mmap<AccessMode, ByteT> &b);

    template<access_mode AccessMode, typename ByteT>
    bool operator!=(const basic_mmap<AccessMode, ByteT> &a,
                    const basic_mmap<AccessMode, ByteT> &b);

    template<access_mode AccessMode, typename ByteT>
    bool operator<(const basic_mmap<AccessMode, ByteT> &a,
                   const basic_mmap<AccessMode, ByteT> &b);

    template<access_mode AccessMode, typename ByteT>
    bool operator<=(const basic_mmap<AccessMode, ByteT> &a,
                    const basic_mmap<AccessMode, ByteT> &b);

    template<access_mode AccessMode, typename ByteT>
    bool operator>(const basic_mmap<AccessMode, ByteT> &a,
                   const basic_mmap<AccessMode, ByteT> &b);

    template<access_mode AccessMode, typename ByteT>
    bool operator>=(const basic_mmap<AccessMode, ByteT> &a,
                    const basic_mmap<AccessMode, ByteT> &b);

    /**
     * This is the basis for all read-only mmap objects and should be preferred over
     * directly using `basic_mmap`.
     */
    template<typename ByteT>
    using basic_mmap_source = basic_mmap<access_mode::read, ByteT>;

    /**
     * This is the basis for all read-write mmap objects and should be preferred over
     * directly using `basic_mmap`.
     */
    template<typename ByteT>
    using basic_mmap_sink = basic_mmap<access_mode::write, ByteT>;

    /**
     * These aliases cover the most common use cases, both representing a raw byte stream
     * (either with a char or an unsigned char/uint8_t).
     */
    using mmap_source = basic_mmap_source<char>;
    using ummap_source = basic_mmap_source<unsigned char>;

    using mmap_sink = basic_mmap_sink<char>;
    using ummap_sink = basic_mmap_sink<unsigned char>;

    /**
     * Convenience factory method that constructs a mapping for any `basic_mmap` or
     * `basic_mmap` type.
     */
    template<
            typename MMap,
            typename MappingToken
    >
    MMap make_mmap(const MappingToken &token,
                   int64_t offset, int64_t length, std::error_code &error) {
        MMap mmap;
        mmap.map(token, offset, length, error);
        return mmap;
    }

    /**
     * Convenience factory method.
     *
     * MappingToken may be a String (`std::string`, `std::string_view`, `const char*`,
     * `std::filesystem::path`, `std::vector<char>`, or similar), or a
     * `mmap_source::handle_type`.
     */
    template<typename MappingToken>
    mmap_source make_mmap_source(const MappingToken &token, mmap_source::size_type offset,
                                 mmap_source::size_type length, std::error_code &error) {
        return make_mmap<mmap_source>(token, offset, length, error);
    }

    template<typename MappingToken>
    mmap_source make_mmap_source(const MappingToken &token, std::error_code &error) {
        return make_mmap_source(token, 0, map_entire_file, error);
    }

/**
 * Convenience factory method.
 *
 * MappingToken may be a String (`std::string`, `std::string_view`, `const char*`,
 * `std::filesystem::path`, `std::vector<char>`, or similar), or a
 * `mmap_sink::handle_type`.
 */
    template<typename MappingToken>
    mmap_sink make_mmap_sink(const MappingToken &token, mmap_sink::size_type offset,
                             mmap_sink::size_type length, std::error_code &error) {
        return make_mmap<mmap_sink>(token, offset, length, error);
    }

    template<typename MappingToken>
    mmap_sink make_mmap_sink(const MappingToken &token, std::error_code &error) {
        return make_mmap_sink(token, 0, map_entire_file, error);
    }

    namespace detail {

#ifdef _WIN32
        namespace win {

/** Returns the 4 upper bytes of an 8-byte integer. */
inline DWORD int64_high(int64_t n) noexcept
{
    return n >> 32;
}

/** Returns the 4 lower bytes of an 8-byte integer. */
inline DWORD int64_low(int64_t n) noexcept
{
    return n & 0xffffffff;
}

inline std::wstring s_2_ws(const std::string& s)
{
    std::wstring ret;
    if (!s.empty())
    {
        ret.resize(s.size());
        int wide_char_count = MultiByteToWideChar(CP_UTF8, 0, s.c_str(),
            static_cast<int>(s.size()), &ret[0], static_cast<int>(s.size()));
        ret.resize(wide_char_count);
    }
    return ret;
}

template<
    typename String,
    typename = typename std::enable_if<
        std::is_same<typename char_type<String>::type, char>::value
    >::type
> file_handle_type open_file_helper(const String& path, const access_mode mode)
{
    return ::CreateFileW(s_2_ws(path).c_str(),
            mode == access_mode::read ? GENERIC_READ : GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            0,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            0);
}

template<typename String>
typename std::enable_if<
    std::is_same<typename char_type<String>::type, wchar_t>::value,
    file_handle_type
>::type open_file_helper(const String& path, const access_mode mode)
{
    return ::CreateFileW(c_str(path),
            mode == access_mode::read ? GENERIC_READ : GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            0,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            0);
}

} // win
#endif // _WIN32

/**
 * Returns the last platform specific system error (errno on POSIX and
 * GetLastError on Win) as a `std::error_code`.
 */
        inline std::error_code last_error() noexcept {
            std::error_code error;
#ifdef _WIN32
            error.assign(GetLastError(), std::system_category());
#else
            error.assign(errno, std::system_category());
#endif
            return error;
        }

        template<typename String>
        file_handle_type open_file(const String &path, const access_mode mode,
                                   std::error_code &error) {
            error.clear();
            if (detail::empty(path)) {
                error = std::make_error_code(std::errc::invalid_argument);
                return invalid_handle;
            }
#ifdef _WIN32
            const auto handle = win::open_file_helper(path, mode);
#else // POSIX
            const auto handle = ::open(c_str(path),
                                       mode == access_mode::read ? O_RDONLY : O_RDWR);
#endif
            if (handle == invalid_handle) {
                error = detail::last_error();
            }
            return handle;
        }

        inline size_t query_file_size(file_handle_type handle, std::error_code &error) {
            error.clear();
#ifdef _WIN32
            LARGE_INTEGER file_size;
    if(::GetFileSizeEx(handle, &file_size) == 0)
    {
        error = detail::last_error();
        return 0;
    }
    return static_cast<int64_t>(file_size.QuadPart);
#else // POSIX
            struct stat sbuf;
            if (::fstat(handle, &sbuf) == -1) {
                error = detail::last_error();
                return 0;
            }
            return sbuf.st_size;
#endif
        }

        struct mmap_context {
            char *data;
            int64_t length;
            int64_t mapped_length;
#ifdef _WIN32
            file_handle_type file_mapping_handle;
#endif
        };

        inline mmap_context memory_map(const file_handle_type file_handle, const int64_t offset,
                                       const int64_t length, const access_mode mode, std::error_code &error) {
            const int64_t aligned_offset = make_offset_page_aligned(offset);
            const int64_t length_to_map = offset - aligned_offset + length;
#ifdef _WIN32
            const int64_t max_file_size = offset + length;
    const auto file_mapping_handle = ::CreateFileMapping(
            file_handle,
            0,
            mode == access_mode::read ? PAGE_READONLY : PAGE_READWRITE,
            win::int64_high(max_file_size),
            win::int64_low(max_file_size),
            0);
    if(file_mapping_handle == invalid_handle)
    {
        error = detail::last_error();
        return {};
    }
    char* mapping_start = static_cast<char*>(::MapViewOfFile(
            file_mapping_handle,
            mode == access_mode::read ? FILE_MAP_READ : FILE_MAP_WRITE,
            win::int64_high(aligned_offset),
            win::int64_low(aligned_offset),
            length_to_map));
    if(mapping_start == nullptr)
    {
        // Close file handle if mapping it failed.
        ::CloseHandle(file_mapping_handle);
        error = detail::last_error();
        return {};
    }
#else // POSIX
            char *mapping_start = static_cast<char *>(::mmap(
                    0, // Don't give hint as to where to map.
                    length_to_map,
                    mode == access_mode::read ? PROT_READ : PROT_WRITE,
                    MAP_SHARED,
                    file_handle,
                    aligned_offset));
            if (mapping_start == MAP_FAILED) {
                error = detail::last_error();
                return {};
            }
#endif
            mmap_context ctx;
            ctx.data = mapping_start + offset - aligned_offset;
            ctx.length = length;
            ctx.mapped_length = length_to_map;
#ifdef _WIN32
            ctx.file_mapping_handle = file_mapping_handle;
#endif
            return ctx;
        }

    } // namespace detail

// -- basic_mmap --

    template<access_mode AccessMode, typename ByteT>
    basic_mmap<AccessMode, ByteT>::~basic_mmap() {
        conditional_sync();
        unmap();
    }

    template<access_mode AccessMode, typename ByteT>
    basic_mmap<AccessMode, ByteT>::basic_mmap(basic_mmap &&other)
            : data_(std::move(other.data_)), length_(std::move(other.length_)),
              mapped_length_(std::move(other.mapped_length_)), file_handle_(std::move(other.file_handle_))
#ifdef _WIN32
            , file_mapping_handle_(std::move(other.file_mapping_handle_))
#endif
            , is_handle_internal_(std::move(other.is_handle_internal_)) {
        other.data_ = nullptr;
        other.length_ = other.mapped_length_ = 0;
        other.file_handle_ = invalid_handle;
#ifdef _WIN32
        other.file_mapping_handle_ = invalid_handle;
#endif
    }

    template<access_mode AccessMode, typename ByteT>
    basic_mmap<AccessMode, ByteT> &
    basic_mmap<AccessMode, ByteT>::operator=(basic_mmap &&other) {
        if (this != &other) {
            // First the existing mapping needs to be removed.
            unmap();
            data_ = std::move(other.data_);
            length_ = std::move(other.length_);
            mapped_length_ = std::move(other.mapped_length_);
            file_handle_ = std::move(other.file_handle_);
#ifdef _WIN32
            file_mapping_handle_ = std::move(other.file_mapping_handle_);
#endif
            is_handle_internal_ = std::move(other.is_handle_internal_);

            // The moved from basic_mmap's fields need to be reset, because
            // otherwise other's destructor will unmap the same mapping that was
            // just moved into this.
            other.data_ = nullptr;
            other.length_ = other.mapped_length_ = 0;
            other.file_handle_ = invalid_handle;
#ifdef _WIN32
            other.file_mapping_handle_ = invalid_handle;
#endif
            other.is_handle_internal_ = false;
        }
        return *this;
    }

    template<access_mode AccessMode, typename ByteT>
    typename basic_mmap<AccessMode, ByteT>::handle_type
    basic_mmap<AccessMode, ByteT>::mapping_handle() const noexcept {
#ifdef _WIN32
        return file_mapping_handle_;
#else
        return file_handle_;
#endif
    }

    template<access_mode AccessMode, typename ByteT>
    template<typename String>
    void basic_mmap<AccessMode, ByteT>::map(const String &path, const size_type offset,
                                            const size_type length, std::error_code &error) {
        error.clear();
        if (detail::empty(path)) {
            error = std::make_error_code(std::errc::invalid_argument);
            return;
        }
        const auto handle = detail::open_file(path, AccessMode, error);
        if (error) {
            return;
        }

        map(handle, offset, length, error);
        // This MUST be after the call to map, as that sets this to true.
        if (!error) {
            is_handle_internal_ = true;
        }
    }

    template<access_mode AccessMode, typename ByteT>
    void basic_mmap<AccessMode, ByteT>::map(const handle_type handle,
                                            const size_type offset, const size_type length, std::error_code &error) {
        error.clear();
        if (handle == invalid_handle) {
            error = std::make_error_code(std::errc::bad_file_descriptor);
            return;
        }

        const auto file_size = detail::query_file_size(handle, error);
        if (error) {
            return;
        }

        if (offset + length > file_size) {
            error = std::make_error_code(std::errc::invalid_argument);
            return;
        }

        const auto ctx = detail::memory_map(handle, offset,
                                            length == map_entire_file ? (file_size - offset) : length,
                                            AccessMode, error);
        if (!error) {
            // We must unmap the previous mapping that may have existed prior to this call.
            // Note that this must only be invoked after a new mapping has been created in
            // order to provide the strong guarantee that, should the new mapping fail, the
            // `map` function leaves this instance in a state as though the function had
            // never been invoked.
            unmap();
            file_handle_ = handle;
            is_handle_internal_ = false;
            data_ = reinterpret_cast<pointer>(ctx.data);
            length_ = ctx.length;
            mapped_length_ = ctx.mapped_length;
#ifdef _WIN32
            file_mapping_handle_ = ctx.file_mapping_handle;
#endif
        }
    }

    template<access_mode AccessMode, typename ByteT>
    template<access_mode A>
    typename std::enable_if<A == access_mode::write, void>::type
    basic_mmap<AccessMode, ByteT>::sync(std::error_code &error) {
        error.clear();
        if (!is_open()) {
            error = std::make_error_code(std::errc::bad_file_descriptor);
            return;
        }

        if (data()) {
#ifdef _WIN32
            if(::FlushViewOfFile(get_mapping_start(), mapped_length_) == 0
           || ::FlushFileBuffers(file_handle_) == 0)
#else // POSIX
            if (::msync(get_mapping_start(), mapped_length_, MS_SYNC) != 0)
#endif
            {
                error = detail::last_error();
                return;
            }
        }
#ifdef _WIN32
        if(::FlushFileBuffers(file_handle_) == 0)
    {
        error = detail::last_error();
    }
#endif
    }

    template<access_mode AccessMode, typename ByteT>
    void basic_mmap<AccessMode, ByteT>::unmap() {
        if (!is_open()) { return; }
        // TODO do we care about errors here?
#ifdef _WIN32
        if(is_mapped())
    {
        ::UnmapViewOfFile(get_mapping_start());
        ::CloseHandle(file_mapping_handle_);
    }
#else // POSIX
        if (data_) { ::munmap(const_cast<pointer>(get_mapping_start()), mapped_length_); }
#endif

        // If `file_handle_` was obtained by our opening it (when map is called with
        // a path, rather than an existing file handle), we need to close it,
        // otherwise it must not be closed as it may still be used outside this
        // instance.
        if (is_handle_internal_) {
#ifdef _WIN32
            ::CloseHandle(file_handle_);
#else // POSIX
            ::close(file_handle_);
#endif
        }

        // Reset fields to their default values.
        data_ = nullptr;
        length_ = mapped_length_ = 0;
        file_handle_ = invalid_handle;
#ifdef _WIN32
        file_mapping_handle_ = invalid_handle;
#endif
    }

    template<access_mode AccessMode, typename ByteT>
    bool basic_mmap<AccessMode, ByteT>::is_mapped() const noexcept {
#ifdef _WIN32
        return file_mapping_handle_ != invalid_handle;
#else // POSIX
        return is_open();
#endif
    }

    template<access_mode AccessMode, typename ByteT>
    void basic_mmap<AccessMode, ByteT>::swap(basic_mmap &other) {
        if (this != &other) {
            using std::swap;
            swap(data_, other.data_);
            swap(file_handle_, other.file_handle_);
#ifdef _WIN32
            swap(file_mapping_handle_, other.file_mapping_handle_);
#endif
            swap(length_, other.length_);
            swap(mapped_length_, other.mapped_length_);
            swap(is_handle_internal_, other.is_handle_internal_);
        }
    }

    template<access_mode AccessMode, typename ByteT>
    template<access_mode A>
    typename std::enable_if<A == access_mode::write, void>::type
    basic_mmap<AccessMode, ByteT>::conditional_sync() {
        // This is invoked from the destructor, so not much we can do about
        // failures here.
        std::error_code ec;
        sync(ec);
    }

    template<access_mode AccessMode, typename ByteT>
    template<access_mode A>
    typename std::enable_if<A == access_mode::read, void>::type
    basic_mmap<AccessMode, ByteT>::conditional_sync() {
        // noop
    }

    template<access_mode AccessMode, typename ByteT>
    bool operator==(const basic_mmap<AccessMode, ByteT> &a,
                    const basic_mmap<AccessMode, ByteT> &b) {
        return a.data() == b.data()
               && a.size() == b.size();
    }

    template<access_mode AccessMode, typename ByteT>
    bool operator!=(const basic_mmap<AccessMode, ByteT> &a,
                    const basic_mmap<AccessMode, ByteT> &b) {
        return !(a == b);
    }

    template<access_mode AccessMode, typename ByteT>
    bool operator<(const basic_mmap<AccessMode, ByteT> &a,
                   const basic_mmap<AccessMode, ByteT> &b) {
        if (a.data() == b.data()) { return a.size() < b.size(); }
        return a.data() < b.data();
    }

    template<access_mode AccessMode, typename ByteT>
    bool operator<=(const basic_mmap<AccessMode, ByteT> &a,
                    const basic_mmap<AccessMode, ByteT> &b) {
        return !(a > b);
    }

    template<access_mode AccessMode, typename ByteT>
    bool operator>(const basic_mmap<AccessMode, ByteT> &a,
                   const basic_mmap<AccessMode, ByteT> &b) {
        if (a.data() == b.data()) { return a.size() > b.size(); }
        return a.data() > b.data();
    }

    template<access_mode AccessMode, typename ByteT>
    bool operator>=(const basic_mmap<AccessMode, ByteT> &a,
                    const basic_mmap<AccessMode, ByteT> &b) {
        return !(a < b);
    }

} // namespace alkaid

