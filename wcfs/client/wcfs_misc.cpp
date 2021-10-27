// Copyright (C) 2019-2021  Nexedi SA and Contributors.
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

#include "wcfs_misc.h"

#include <golang/libgolang.h>
#include <golang/errors.h>
#include <golang/fmt.h>
#include <golang/io.h>
using namespace golang;

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <memory>

// golang::
namespace golang {

// os::
namespace os {

// TODO -> os.PathError + err=syscall.Errno
static error _pathError(const char *op, const string &path, int syserr);
static string _sysErrString(int syserr);

int     _File::fd() const   { return _fd; }
string  _File::name() const { return _path; }

_File::_File() {}
_File::~_File() {}
void _File::decref() {
    if (__decref())
        delete this;
}


tuple<File, error> open(const string &path, int flags, mode_t mode) {
    int fd = ::open(path.c_str(), flags, mode);
    if (fd == -1)
        return make_tuple(nil, _pathError("open", path, errno));

    File f = adoptref(new _File);
    f->_path = path;
    f->_fd   = fd;
    return make_tuple(f, nil);
}

error _File::close() {
    _File& f = *this;

    int err = ::close(f._fd);
    if (err != 0)
        return f._errno("close");
    f._fd = -1;
    return nil;
}

tuple<int, error> _File::read(void *buf, size_t count) {
    _File& f = *this;
    int n;

    n = ::read(f._fd, buf, count);
    if (n == 0)
        return make_tuple(n, io::EOF_);
    if (n < 0)
        return make_tuple(0, f._errno("read"));

    return make_tuple(n, nil);
}

tuple <int, error> _File::write(const void *buf, size_t count) {
    _File& f = *this;
    int n, wrote=0;

    // NOTE contrary to write(2) we have to write all data as io.Writer requires.
    while (count != 0) {
        n = ::write(f._fd, buf, count);
        if (n < 0)
            return make_tuple(wrote, f._errno("write"));

        wrote += n;
        buf    = ((const char *)buf) + n;
        count -= n;
    }

    return make_tuple(wrote, nil);
}

error _File::stat(struct stat *st) {
    _File& f = *this;

    int err = fstat(f._fd, st);
    if (err != 0)
        return f._errno("stat");
    return nil;
}


// _errno returns error corresponding to op(file) and errno.
error _File::_errno(const char *op) {
    _File& f = *this;
    return _pathError(op, f._path, errno);
}

// _pathError returns os.PathError-like for op/path and system error
// indicated by syserr.
static error _pathError(const char *op, const string &path, int syserr) {
    // TODO v(_sysErrString(syserr)) -> v(syscall.Errno(syserr))
    return fmt::errorf("%s %s: %s", op, v(path), v(_sysErrString(syserr)));
}


// _sysErrString returns string corresponding to system error syserr.
static string _sysErrString(int syserr) {
    char ebuf[128];
    char *estr = strerror_r(syserr, ebuf, sizeof(ebuf));
    return string(estr);
}

}   // os::


// xstrconv::   (strconv-like)
namespace xstrconv {

// parseHex64 decodes 16-character-wide hex-encoded string into uint64.
tuple<uint64_t, error> parseHex64(const string& s) {
    if (s.size() != 16)
        return make_tuple(0, fmt::errorf("hex64 %s invalid", v(s)));

    uint64_t v;
    int n = sscanf(s.c_str(), "%16" SCNx64, &v);
    if (n != 1)
        return make_tuple(0, fmt::errorf("hex64 %s invalid", v(s)));

    return make_tuple(v, nil);
}

// parseInt decodes string s as signed decimal integer.
tuple<int64_t, error> parseInt(const string& s) {
    int64_t v;
    int n = sscanf(s.c_str(), "%" SCNi64, &v);
    if (!(n == 1 && std::to_string(v) == s))
        return make_tuple(0, fmt::errorf("int %s invalid", v(s)));
    return make_tuple(v, nil);
}

// parseUint decodes string s as unsigned decimal integer.
tuple<uint64_t, error> parseUint(const string& s) {
    uint64_t v;
    int n = sscanf(s.c_str(), "%" SCNu64, &v);
    if (!(n == 1 && std::to_string(v) == s))
        return make_tuple(0, fmt::errorf("uint %s invalid", v(s)));
    return make_tuple(v, nil);
}

}   // xstrconv::


}   // golang::


// xerr::
namespace xerr {

// XXX don't require fmt::vsprintf
#if 0
Contextf::Contextf(const char *format, ...) {
    Contextf& c = *this;

    va_list argp;
    va_start(argp, format);
    c.errctx = fmt::sprintfv(format, argp);
    va_end(argp);
}
#endif

error Contextf::operator() (error err) const {
    const Contextf& c = *this;

    if (err == nil)
        return nil;

    return fmt::errorf("%s: %w", v(c.errctx), err);
}

}   // xerr::


#include <golang/time.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/syscall.h>

// golang::log::
namespace golang {
namespace log {

void __Logf(const char *file, int line, char level, const char *format, ...) {
    double t = time::now();
    time_t t_int = time_t(t);
    struct tm tm_loc;
    localtime_r(&t_int, &tm_loc);

    char t_buf[32];
    strftime(t_buf, sizeof(t_buf), "%m%d %H:%M:%S", &tm_loc);

    int t_us = int((t-t_int)*1E6);

    pid_t tid = syscall(SYS_gettid);

    string prefix = fmt::sprintf("%c%s.%06d % 7d %s:%d] ", level, t_buf, t_us, tid, file, line);
    // TODO better to emit prefix and msg in one go.
    flockfile(stderr);
    fprintf(stderr, "%s", v(prefix));

    va_list argp;
    va_start(argp, format);
    vfprintf(stderr, format, argp);
    va_end(argp);

    fprintf(stderr, "\n");
    funlockfile(stderr);
}

}}  // golang::log::


// wcfs::
namespace wcfs {

template<> string v_(error err) {
    return (err != nil) ? err->Error() : "nil";
}

static string h016(uint64_t v)              { return fmt::sprintf("%016lx", v); }
template<> string v_(const zodb::Tid& tid)  { return h016(tid);                 }
//template<> string v_(zodb::Oid oid) { return h016(oid);                 }
// XXX Tid and Oid are typedefs for uint64_t and C++ reduces template
// specializations to the underlying type. This providing specialization for
// both Tid and Oid results in "multiple definition" error.

}   // wcfs::
