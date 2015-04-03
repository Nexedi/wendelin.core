# Wendelin.bigfile | benchmarks for zodb backend
#
# TODO text
from wendelin.bigfile.file_zodb import ZBigFile
from wendelin.lib.mem import memset
from wendelin.lib.testing import Adler32, nulladler32_bysize, ffadler32_bysize
from wendelin.bigfile.tests.common_zodb import dbopen, dbclose
import transaction
from tempfile import mkdtemp
from shutil import rmtree

tmpd = None
from wendelin.bigfile.tests.bench_0virtmem import filesize, blksize # to get comparable timings
blen = filesize // blksize
nulladler32 = nulladler32_bysize(blen * blksize)
ffadler32   = ffadler32_bysize(blen * blksize)


def setup_module():
    global tmpd
    tmpd = mkdtemp('', 'bigzodb.')

    root = dbopen('%s/1.fs' % tmpd)
    root['zfile'] = ZBigFile(blksize)
    transaction.commit()

    dbclose(root)


def teardown_module():
    rmtree(tmpd)


# NOTE runs before _writeff
def bench_bigz_readhole():  _bench_bigz_hash(Adler32,   nulladler32)

def bench_bigz_writeff():
    root = dbopen('%s/1.fs' % tmpd)
    f   = root['zfile']
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    memset(vma, 0xff)
    transaction.commit()

    del vma # TODO vma.close()
    del fh  # TODO fh.close()
    del f   # XXX  f.close() ?
    dbclose(root)


def _bench_bigz_hash(hasher, expect):
    root = dbopen('%s/1.fs' % tmpd)
    f   = root['zfile']
    fh  = f.fileh_open()    # TODO + ram
    vma = fh.mmap(0, blen)  # XXX assumes blksize == pagesize

    h = hasher()
    h.update(vma)

    del vma # vma.close()
    del fh  # fh.close()
    del f   # f.close()
    dbclose(root)
    assert h.digest() == expect


def bench_bigz_read():      _bench_bigz_hash(Adler32,   ffadler32)
