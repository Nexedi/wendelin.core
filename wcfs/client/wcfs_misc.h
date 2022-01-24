// Copyright (C) 2019-2022  Nexedi SA and Contributors.
//                          Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

// wcfs_misc.{h,cpp} provide miscellaneous utilities for other wcfs_* files.

#ifndef _NXD_WCFS_MISC_H_
#define _NXD_WCFS_MISC_H_

// XXX hack: C++ does not have __builtin_types_compatible_p, but CCAN configure
// thinks it does because CCAN is configured via C, not C++.
#include <config.h>
#undef  HAVE_BUILTIN_TYPES_COMPATIBLE_P
#define HAVE_BUILTIN_TYPES_COMPATIBLE_P 0
#include <ccan/array_size/array_size.h>

#include <stddef.h>
#include <stdint.h>

#include <golang/libgolang.h>
using namespace golang;

#include <string>
using std::string;

#include <utility>
using std::pair;
using std::make_pair;

#include <tuple>
using std::tuple;
using std::make_tuple;
using std::tie;

#include <vector>
using std::vector;


#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

// xgolang::
namespace xgolang {

// xos::
namespace xos {

extern global<error> ErrClosed;


// os::File mimics os.File from Go.
// its operations return error with full file context.
typedef refptr<class _File> File;
class _File : public object {
    int    _fd;
    string _path;

    // don't new - create via open
private:
    _File();
    ~_File();
    friend tuple<File, error> Open(const string &path, int flags, mode_t mode);
public:
    void decref();

public:
    int     _sysfd() const;
    string  Name()   const;
    error   Close();

    // read implements io.Reader from Go: it reads into buf up-to count bytes.
    // XXX buf -> slice<byte> ?
    tuple<int, error> Read(void *buf, size_t count);

    // write implements io.Writer from Go: it writes all data from buf.
    //
    // NOTE write behaves like io.Writer in Go - it tries to write as much
    // bytes as requested, and if it could write only less - it returns error.
    // XXX buf -> slice<byte> ?
    tuple<int, error> Write(const void *buf, size_t count);

    error   Stat(struct stat *st);

private:
    error _errno(const char *op);
};

// Open opens file @path.
tuple<File, error> Open(const string &path, int flags = O_RDONLY,
        mode_t mode = S_IRUSR | S_IWUSR | S_IXUSR |
                      S_IRGRP | S_IWGRP | S_IXGRP |
                      S_IROTH | S_IWOTH | S_IXOTH);


// afterfork

// IAfterFork is the interface that objects must implement to be notified after fork.
typedef refptr<struct _IAfterFork> IAfterFork;
struct _IAfterFork : public _interface {
    // afterFork is called in just forked child process for objects that
    // were previously registered in parent via RegisterAfterFork.
    virtual void afterFork() = 0;
};

// RegisterAfterFork registers obj so that obj.afterFork is run after fork in
// the child process.
void RegisterAfterFork(IAfterFork obj);

// UnregisterAfterFork undoes RegisterAfterFork.
// It is noop if obj was not registered.
void UnregisterAfterFork(IAfterFork obj);

}   // xos::

// xmm::
namespace xmm {
    tuple<uint8_t*, error> map(int prot, int flags, xos::File f, off_t offset, size_t size);
    error map_into(void *addr, size_t size, int prot, int flags, xos::File f, off_t offset);
    error unmap(void *addr, size_t size);

}   // xmm::


// io::ioutil::
namespace io {
namespace ioutil {

tuple<string, error> ReadFile(const string& path);

}}  // io::ioutil::

// ---- misc ----


// xstrconv::
namespace xstrconv {

tuple<uint64_t, error> parseHex64(const string& s);
tuple<int64_t,  error> parseInt(const string& s);
tuple<uint64_t, error> parseUint(const string& s);

}   // xstrconv::

// xlog::
namespace xlog {

#define Debugf(format, ...) __Logf(__FILE__, __LINE__, 'D', format, ##__VA_ARGS__)
#define Infof(format, ...)  __Logf(__FILE__, __LINE__, 'I', format, ##__VA_ARGS__)
#define Warnf(format, ...)  __Logf(__FILE__, __LINE__, 'W', format, ##__VA_ARGS__)
#define Errorf(format, ...) __Logf(__FILE__, __LINE__, 'E', format, ##__VA_ARGS__)
#define Fatalf(format, ...) __Logf(__FILE__, __LINE__, 'F', format, ##__VA_ARGS__)
void __Logf(const char *file, int line, char level, const char *format, ...);

}   // xlog::

}   // xgolang::


// zodb::
namespace zodb {

typedef uint64_t Tid;
typedef uint64_t Oid;

}   // zodb::


#include <golang/fmt.h>

// xerr::
namespace xerr {

// xerr::Contextf mimics xerr.Contextf from Go.
//
// Usage is a bit different(*) compared to Go:
//
//  func doSomething(arg) {
//      xerr.Contextf E("doing something %s", v(arg));
//      ...
//      return E(err);
//  }
//
// (*) because C++ does not allow to modify returned value on the fly.
class Contextf {
    string errctx;

public:
    template<typename ...Argv>
    inline Contextf(const char *format, Argv... argv) {
        // XXX string() to avoid "error: format not a string literal" given by -Werror=format-security
        errctx = fmt::sprintf(string(format), argv...);
    }

    error operator() (error) const;
};

}   // xerr::


// wcfs::
namespace wcfs {

// TidHead is invalid Tid which is largest Tid value and means @head.
const zodb::Tid TidHead = -1ULL;


// v mimics %v for T to be used in printf & friends.
//
// NOTE returned char* pointer is guaranteed to stay valid only till end of
// current expression. For example
//
//      printf("hello %s", v(obj))
//
// is valid, while
//
//      x = v(obj);
//      use(x);
//
// is not valid.
#define v(obj)  (wcfs::v_(obj).c_str())
template<typename T> string v_(T* obj)          { return obj->String(); }
template<typename T> string v_(const T* obj)    { return obj->String(); }
template<typename T> string v_(const T& obj)    { return obj.String();  }
template<typename T> string v_(refptr<T> obj)   { return obj->String(); }

template<> inline string v_(const string& s) { return s; }
template<> string v_(error);
template<> string v_(const zodb::Tid&);
template<> string v_(const zodb::Oid&);

}   // wcfs::

#endif
