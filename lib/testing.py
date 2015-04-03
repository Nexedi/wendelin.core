# Wendelin. Testing utilities
# Copyright (C) 2014-2015  Nexedi SA and Contributors.
#                          Kirill Smelkov <kirr@nexedi.com>
#
# This program is free software: you can Use, Study, Modify and Redistribute
# it under the terms of the GNU General Public License version 3, or (at your
# option) any later version, as published by the Free Software Foundation.
#
# You can also Link and Combine this program with other software covered by
# the terms of any of the Open Source Initiative approved licenses and Convey
# the resulting work. Corresponding source of such a combination shall include
# the source code for all other software used.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See COPYING file for full licensing terms.

from zlib import adler32
from struct import pack
import codecs
import math

# hashlib-like interface to adler32
class Adler32:
    def __init__(self):
        self.v = adler32(b'')

    def update(self, data):
        self.v = adler32(data, self.v)

    def digest(self):
        return pack('>I', self.v & 0xffffffff)    # see zlib docs about 0xffffffff


def _hex(*args):
    return tuple(codecs.decode(_, 'hex') for _ in args)

# adler32('\0' * 2^n)
_nulladler32_byorder = _hex(
    '00010001',    # 0
    '00020001',    # 1
    '00040001',    # 2
    '00080001',    # 3
    '00100001',    # 4
    '00200001',    # 5
    '00400001',    # 6
    '00800001',    # 7
    '01000001',    # 8
    '02000001',    # 9
    '04000001',    # 10
    '08000001',    # 11
    '10000001',    # 12
    '20000001',    # 13
    '40000001',    # 14
    '80000001',    # 15
    '000f0001',    # 16
    '001e0001',    # 17
    '003c0001',    # 18
    '00780001',    # 19
    '00f00001',    # 20
    '01e00001',    # 21
    '03c00001',    # 22
    '07800001',    # 23
    '0f000001',    # 24
    '1e000001',    # 25
    '3c000001',    # 26
    '78000001',    # 27
    'f0000001',    # 28
    'e00f0001',    # 29
    'c02d0001',    # 30
)

# adler32('\xff' * 2^n)
_ffadler32_byorder = _hex(
    '01000100',    # 0
    '02ff01ff',    # 1
    '09fa03fd',    # 2
    '23e407f9',    # 3
    '87880ff1',    # 4
    '0e2e1fe1',    # 5
    '18983fc1',    # 6
    '22207f81',    # 7
    '0800ff01',    # 8
    '1ef1fe10',    # 9
    '79a6fc2e',    # 10
    'e26bf86a',    # 11
    '8161f0e2',    # 12
    'f4a3e1d2',    # 13
    'b0d9c3b2',    # 14
    '7fc28772',    # 15
    '77970ef2',    # 16
    'cf5c1de3',    # 17
    '1f7f3bc5',    # 18
    '41c07789',    # 19
    '8e88ef11',    # 20
    '493fde30',    # 21
    '430dbc6e',    # 22
    '484778ea',    # 23
    '9933f1d3',    # 24
    '5509e3b4',    # 25
    '3471c776',    # 26
    '92408efa',    # 27
    'ca071e02',    # 28
    '2a2a3c03',    # 29
    'ac6a7805',    # 30
)


# md5sum('\0' * (2^n))
_nullmd5_byorder = _hex(
    '93b885adfe0da089cdf634904fd59f71',    # 0
    'c4103f122d27677c9db144cae1394a66',    # 1
    'f1d3ff8443297732862df21dc4e57262',    # 2
    '7dea362b3fac8e00956a4952a3d4f474',    # 3
    '4ae71336e44bf9bf79d2752e234818a5',    # 4
    '70bc8f4b72a86921468bf8e8441dce51',    # 5
    '3b5d3c7d207e37dceeedd301e35e2e58',    # 6
    'f09f35a5637839458e462e6350ecbce4',    # 7
    '348a9791dc41b89796ec3808b5b5262f',    # 8
    'bf619eac0cdf3f68d496ea9344137e8b',    # 9
    '0f343b0931126a20f133d67c2b018a3b',    # 10
    'c99a74c555371a433d121f551d6c6398',    # 11
    '620f0b67a91f7f74151bc5be745b7110',    # 12
    '0829f71740aab1ab98b33eae21dee122',    # 13
    'ce338fe6899778aacfc28414f2d9498b',    # 14
    'bb7df04e1b0a2570657527a7e108ae23',    # 15
    'fcd6bcb56c1689fcef28b57c22475bad',    # 16
    '0dfbe8aa4c20b52e1b8bf3cb6cbdf193',    # 17
    'ec87a838931d4d5d2e94a04644788a55',    # 18
    '59071590099d21dd439896592338bf95',    # 19
    'b6d81b360a5672d80c27430f39153e2c',    # 20
    'b2d1236c286a3c0704224fe4105eca49',    # 21
    'b5cfa9d6c8febd618f91ac2843d50a1c',    # 22
    '96995b58d4cbf6aaa9041b4f00c7f6ae',    # 23
    '2c7ab85a893283e98c931e9511add182',    # 24
    '58f06dd588d8ffb3beb46ada6309436b',    # 25
    '7f614da9329cd3aebf59b91aadc30bf0',    # 26
    'fde9e0818281836e4fc0edfede2b8762',    # 27
    '1f5039e50bd66b290c56684d8550c6c2',    # 28
    'aa559b4e3523a6c931f08f4df52d58f2',    # 29
    'cd573cfaace07e7949bc0c46028904ff',    # 30
)


def ilog2_exact(x):
    xlog2 = int(math.log(x, 2))
    if x != (1 << xlog2):
        raise ValueError('Only 2^n supported')
    return xlog2

def nulladler32_bysize(size):   return _nulladler32_byorder [ilog2_exact(size)]
def nullmd5_bysize(size):       return _nullmd5_byorder     [ilog2_exact(size)]

def ffadler32_bysize(size):     return _ffadler32_byorder   [ilog2_exact(size)]
