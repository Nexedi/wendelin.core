/* Demo program that shows how to release memory on shmfs (=tmpfs) via
 * FALLOC_FL_PUNCH_HOLE - watch how it runs with
 *
 *  `watch -n1 df -h /dev/shm/`
 *
 *
 * NOTE if WORK > free ram dedicated to /dev/shm we get SIGBUS:
 *
 *      via this way kernel tells userspace that there is no more space in
 *      backing store attached to mapping.
 *
 *      Strategy -> free some memory on that filesystem (in particular at that
 *      file) and retry
 *
 *
 * NOTE hugetlbfs (as of 3.19-rc1) supports sparse files but does not support holepunch.
 *
 *      1) sparse files:
 *
 *          $ cd /dev/hugepages/
 *          $ truncate -s 128T x
 *          $ ls -lh x
 *          ... 128T ... x
 *          $ du -sh x
 *          0       x
 *
 *          # then one can mmap some inner part of it, e.g. 4M-8M and use that
 *          # and only that memory will be allocated.
 *
 *
 *      2) holepunch: it supports punching holes at file-end though (i.e. ftruncate
 *      works) and I've digged through sys_truncate() and its support for hugetlbfs
 *      in hugetlbfs_setattr() and it looks like extending this support to cover
 *      "truncating range" (i.e. holepunch) should not be that hard.
 *
 *      -> TODO fix hugetlbfs
 *
 *      NOTE care should be taken to correctly maintain huge-pages reservation
 *      numbers etc (HugePages_Rsvd in /proc/meminfo) as hugetlbfs plays own
 *      games with reservation on each mmap to be able to promise not to get
 *      SIGBUS at later page access.
 *
 *      https://lkml.org/lkml/2011/11/16/499
 */
#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/user.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <err.h>
#include <errno.h>
#include <stdint.h>
#include <signal.h>
#include <stdlib.h>

#undef  NDEBUG
#include <assert.h>

#define KB      (1024ULL)
#define MB      (1024*KB)
#define GB      (1024*MB)
#define TB      (1024*GB)
#define PB      (1024*TB)
#define EB      (1024*PB)
//#define ZB      (1024*EB)
//#define YB      (1024*ZB)


#define VIRTMAX     (64*TB)     /* 2^46 - address space limit on linux/x86_64 */
#define FILEMAX     (8*EB-1)    /* 2^64 - file size limit + off_t is signed -> 2^63 */

#define WORK        (16*GB)

#define RECLAIM_BATCH   (128*256)   /* how many pages to free at once (= 128MB) */


/* string for si_code on SIGBUS */
const char *buscode(int si_code)
{
#define E(code, text)   if (si_code == code) return text
    E(BUS_ADRALN,       "invalid address alignment");
    E(BUS_ADRERR,       "non-existent physical address");
    E(BUS_OBJERR,       "object specific hardware error");
    E(BUS_MCEERR_AR,    "hardware memory error consumed on a machine check: action required");
    E(BUS_MCEERR_AO,    "hardware memory error detected in process but not consumed: action optional");

    return NULL;    // crash
}



int fd;
/* where allocated area for work starts (may grow up, as we free lru memory) */
size_t work_alloc_start;
uint8_t *p;
size_t pagesize;


/* simple memory reclaim on SIGBUS */
void sigbus_handler(int sig, siginfo_t *si, void *_uc)
{
    int save_errno = errno;
    int e;
    // si_code      BUS_ADRALN BUS_ADDRERR BUS_OBJERR BUS_MCEERR_*
    // si_trapno    - not supported on x86_64
    // si_addr_lsb  - not set except for BUS_MCERR_*  (memory hw failure)

    /* handle only "nonexistent physical address" - this way filesystems report
     * that there is no more space in backing store */
    if (si->si_code != BUS_ADRERR)
        goto dont_handle;

    /* in general we should look for si->si_addr to determine which mapping and
     * in turn fs/file tells us, but here we know it already */
    assert( ((void *)p <= si->si_addr)  &&  (si->si_addr < (void *)(p + WORK)) );

    /* deallocate some batch of touched pages, starting from older */
    fprintf(stderr, "\tfreeing %i pages @ P%lx  (%lx)...\n",
            RECLAIM_BATCH, work_alloc_start / pagesize, work_alloc_start);
    e = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            work_alloc_start, RECLAIM_BATCH * pagesize);
    if (e)
        err(errno, "holepunch");

    work_alloc_start += RECLAIM_BATCH * pagesize;

    errno = save_errno;
    return;

dont_handle:
    fprintf(stderr, "SIGBUS si_code: %x (%s)\n", si->si_code, buscode(si->si_code));
    fprintf(stderr, "? unexpected sigbus - abort\n");
    abort();
}


void verify(const char *mntpt, size_t pgsize)
{
    char filename[128];
    size_t i;
    int e;

    pagesize = pgsize;
    work_alloc_start = 0;
    fd = -1;
    p = NULL;

    fprintf(stderr, "\nVerifying %s  pagesize: %lu ...\n", mntpt, pagesize);

    snprintf(filename, sizeof(filename), "%s/t", mntpt);
    fd = open(filename, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd==-1)
        err(errno, "open");

    e = ftruncate(fd, FILEMAX);
    if (e)
        err(errno, "ftruncate");

    p = mmap(NULL, VIRTMAX, PROT_READ | PROT_WRITE,
            MAP_SHARED, fd, 0);
    if (p == MAP_FAILED)
        err(errno, "mmap");

    /* touch WORK */
    fprintf(stderr, "allocating...\n");
    for (i=0; i<WORK; i += pagesize) {
        p[i] = 1;

        //if (i%50 == 0)
        //    usleep(1);
    }
    fprintf(stderr, "\t... done\n");
    //assert(p[0] == 1);    /* first pages may be already freed */
    assert(p[work_alloc_start] == 1);

    /* hole punch touched memory */
    fprintf(stderr, "deallocating...\n");
    for (i=0; i<WORK; i += pagesize) {
        e = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                i, pagesize);
        if (e)
            err(errno, "fallocate");

        //if (i%50 == 0)
        //    usleep(1);
    }
    fprintf(stderr, "\t... done\n");
    assert(p[0] == 0);  /* changes must be forgotten */
    assert(p[work_alloc_start] == 0);

    e = munmap(p, VIRTMAX);
    if (e)
        err(errno, "munmap");

    e = close(fd);
    if (e)
        err(errno, "close");

    fprintf(stderr, "OK\n");
}


int main()
{
    int e;

    /* prepare to catch SIGBUS */
    struct sigaction sa;
    sa.sa_sigaction = sigbus_handler;
    sa.sa_flags     = SA_SIGINFO;
    e = sigemptyset(&sa.sa_mask);
    if (e)
        err(errno, "sigemptyset");
    e = sigaction(SIGBUS, &sa, NULL);
    if (e)
        err(errno, "sigaction");


    verify("/dev/shm",       PAGE_SIZE);

    /* does not work as of 3.17-rc3. Problems with hugetlbfs:
     *
     * 1) does not support sparse files - it is not possible to ftruncate a
     *    file bigger than nr_hugepages;
     *
     * 2) does not support fallocate.
     */
    verify("/dev/hugepages", 2*MB);         // XXX HPAGE_SIZE hardcoded

    return 0;
}
