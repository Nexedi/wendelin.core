/* XXX MAP_ANONYMOUS | MAP_SHARED is not really anonymous - the kernel
 *     internally opens a file for such mappings on shmfs (=tmpfs), or hugetlbfs for
 *     MAP_HUGETLB
 *
 *     -> this is not memory aliasing for anonymous memory - this is equivalent
 *        to usual file mappings into multiple addresses with the same offset.
 *
 *     -> memory aliasing for MAP_ANONYMOUS | MAP_PRIVATE memory is not
 *        possible as of 3.17-rc1.
 *
 *        The most close thing so far is out-of-tree patch for remap_anon_pages()
 *        from Andrea Arcangeli - see http://lwn.net/Articles/550555/ (from 2013,
 *        but it gives the idea; the patch is being continuously updated at
 *        git://git.kernel.org/pub/scm/linux/kernel/git/andrea/aa.git)
 *
 *        updates:
 *
 *          http://lwn.net/Articles/615086/     2014 Oct
 *          http://lwn.net/Articles/636226/     2015 March
 *
 *        and remap_anon_pages() is going away because Linus dislikes it.
 *
 * Unfortunately, as of 3.17-rc1 transparent huge pages (THP) work only with
 * private anonymous mappings...
 *
 * Resume: in order to use hugepages and aliasing, for now we have to stick to
 * allocating pages from files on shmfs and hugetlbfs and do our own management
 * with those. Sigh...
 *
 * Original text follows :
 *
 * ---- 8< ----
 * Demo program, that shows how to mmap-alias two memory areas for anonymous memory.
 *
 * The way it works is through undocumented-in-man, but well-fixed and
 * documented in kernel source
 *
 *      mremap(old_addr, old_size, new_size, MREMAP_FIXED, new_addr)
 *
 * behaviour - if old_size=0, then old_addr is NOT unmapped, and as the result
 * we get two mapping pointing to the same physical memory.
 *
 * experimentally observed that for the trick to work, original mapping has to
 * be obtained with MAP_SHARED - with MAP_PRIVATE old_addr is not unmapped but
 * instead of alias, new zero page is returned (maybe need to investigate - THP
 * currently only work with MAP_PRIVATE mappings).
 *
 * References
 * ----------
 *
 *  https://git.kernel.org/cgit/linux/kernel/git/history/history.git/commit/?id=4547e81c1f3e35dc47c4bfbfd3444cb0401c2b0b
 *  commit 4547e81c1f3e35dc47c4bfbfd3444cb0401c2b0b
 *  Author: Linus Torvalds <torvalds@evo.osdl.org>
 *  Date:   Mon Jan 12 01:13:40 2004 -0800
 *
 *      Dosemu actually wants to do a zero-sized source mremap
 *      to generate the duplicate area at 0x0000 and 0x100000.
 *
 *      There's no downside to it, so allow it even though it's
 *      a tad strange.
 *
 *  http://sourceforge.net/p/dosemu/code/ci/master/tree/src/arch/linux/mapping/mapshm.c#l23
 *  """The trick is to set old_len = 0,
 *  this won't unmap at the old address, but with
 *  shared mem the 'nopage' vm_op will map in the right
 *  pages. We need however to take care not to map
 *  past the end of the shm area"""
 */
#define _GNU_SOURCE
#include <sys/mman.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

#undef NDEBUG
#include <assert.h>


void die(const char *msg)
{
    perror(msg);
    exit(1);
}


void verify(const char *title, size_t len, int mmap_flags)
{
    uint8_t *P1, *P2, *P;
    int err;

    printf("Verifying %s (len=%lx\tflags=%x) ...", title, len, mmap_flags);
    fflush(stdout);

    /* first page with R/W */
    P1 = mmap(NULL, len, PROT_READ | PROT_WRITE,
                MAP_SHARED /* <- important; for MAP_PRIVATE the trick does not work */
                | mmap_flags
                | MAP_ANONYMOUS, -1, 0);
    if (P1 == MAP_FAILED)
        die("mmap P1");

    P1[0] = 0;
    P1[1] = 1;
    P1[2] = 2;
    P1[len-1] = 99;

    /* second page - just address space so far */
    P2 = mmap(NULL, len, PROT_NONE,
                MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (P2 == MAP_FAILED)
        die("mmap P2");


    /*
     * mmap-alias P1 to P2. The trick is to pass 0 as old_size - then P1 won't
     * be unmapped.
     */
    P = mremap(P1, 0/*old_size - important!*/, len,
                MREMAP_FIXED | MREMAP_MAYMOVE, P2);
    if (P == MAP_FAILED)
        die("mremap P1->P2");

    assert(P == P2);

    /* P1 should still be mapped (and contain old values) */
    assert (P1[0] == 0);
    assert (P1[1] == 1);
    assert (P1[2] == 2);
    assert (P1[len-1] == 99);

    /* P2 should be mapped too */
    assert (P2[0] == 0);
    assert (P2[1] == 1);
    assert (P2[2] == 2);
    assert (P2[len-1] == 99);

    /* verify changes propagate back and forth */
    P2[0] = 11; assert (P1[0] == 11);
    P1[1] = 12; assert (P2[1] == 12);
    P2[len-1] = 100; assert (P1[len-1] == 100);
    P1[len-2] =  88; assert (P2[len-2] ==  88);

    err = munmap(P1, len);
    if (err < 0)
        die("munmap P1");

    /* verify P2 is still there */
    assert (P2[0] == 11);
    assert (P2[1] == 12);
    assert (P2[len-2] ==  88);
    assert (P2[len-1] == 100);

    err = munmap(P2, len);
    if (err < 0)
        die("munmap P2");

    printf("\tOK\n");
}


int main()
{
    size_t pagesize = 4096;         // XXX hardcoded
    size_t pagehuge = 2*1024*1024;  // XXX hardcoded

    verify("std pages ", 4*pagesize, 0);

    /*
     * NOTE(2admin) By default # of ready hugepages is 0. Adjust
     *
     *      /proc/sys/vm/nr_hugepages
     *
     * or more explicitly
     *
     *      /sys/kernel/mm/hugepages/hugepages-<size>/nr_hugepages
     *
     * before trying to allocate them.
     */
    // FIXME for this mremap fails with EINVAL  - explicitly disabled in
    // mremap::vma_to_resize(). Changing this would need thorough understanding
    // of linux mm, which is out of scope for now.
    verify("huge pages", 4*pagehuge, MAP_HUGETLB);

    return 0;
}
