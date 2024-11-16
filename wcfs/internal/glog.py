# Copyright (C) 2024-2025  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Free Software licenses or any of the Open Source
# Initiative approved licenses and Convey the resulting work. Corresponding
# source of such a combination shall include the source code for all other
# software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.
# See https://www.nexedi.com/licensing for rationale and options.

"""Package glog provides glog-style logging for Python.

It merely wraps and adjusts standard package logging to do so:

- use glog.basicConfig to setup logging
- use standard logging.* functions do emit log messages
"""

from __future__ import print_function, absolute_import

import logging, time
from wendelin.wcfs.internal import os as xos


# basicConfig is like logging.basicConfig but configures log output format to match glog.
def basicConfig(stream, level):
    logging.setLogRecordFactory(LogRecord)
    h = logging.StreamHandler(stream)
    f = Formatter()
    h.setFormatter(f)
    logging.root.addHandler(h)


# LogRecord amends logging.LogRecord to also include .thread_id attribute.
class LogRecord(logging.LogRecord):
    def __init__(self, *argv, **kw):
        logging.LogRecord.__init__(self, *argv, **kw)
        self.thread_id = xos.gettid()


# Formatter adjusts logging.Formatter to behave similarly to glog.
#
# it depends on LogRecord to be used instead of logging.LogRecord as the log record factory.
class Formatter(logging.Formatter):
    def __init__(self):
        logging.Formatter.__init__(self,
            "%(levelchar)s%(asctime)s %(thread_id)d %(filename)s:%(lineno)d] %(name)s: %(message)s",
            "%m%d %H:%M:%S")

    # provide .levelchar to record
    def formatMessage(self, record):
        record.levelchar = record.levelname[0]
        return logging.Formatter.formatMessage(self, record)

    # adjust emitted time to include microseconds
    def formatTime(self, record, datefmt):
        s, xs = divmod(record.created, 1)
        us = int(round(xs*1e6))
        t = time.strftime(datefmt, self.converter(s))
        t += ".%06d" % us
        return t
