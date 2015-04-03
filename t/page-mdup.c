/* Demo program, that shows 2 memory pages can be combined into 1 bigger
 * _contiguous_ memory area via shm / mmap. The idea is that this way we'll
 * combine array pages into larger slice on client __getslice__ requests and
 * the result would be usual contiguous ndarray while pages of it could live in
 * different places in memory.
 *
 * Unfortunately there is no way to mmap-duplicate pages for MAP_ANONYMOUS, so
 * the way it is done is via a file in tmpfs (on /dev/shm/ via posix shm):
 *
 * https://groups.google.com/forum/#!topic/comp.os.linux.development.system/Prx7ExCzsv4
 */
#include <sys/mman.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>


#define TRACE(msg, ...) do { \
    fprintf(stderr, msg, ##__VA_ARGS__);   \
    fprintf(stderr, "\n");  \
} while (0)

void die(const char *msg)
{
    perror(msg);
    exit(1);
}


int main()
{
    uint8_t *page1, *page2, *page12, *p;
    size_t len;
    int f, err;

    len = 1*4096;   /* XXX = 1 page */


    /* TODO - choose name a-la mktemp and loop changing it if EEXIST */
    f = shm_open("/array", O_RDWR | O_CREAT | O_EXCL,
                            S_IRUSR | S_IWUSR);
    if (f < 0)
        die("shm_open");

    /*
     * unlink so that the file is removed on only memory mapping(s) are left.
     * All mappings will be released upon program exit and so the memory
     * resources would release too
     */
    err = shm_unlink("/array");
    if (err)
        perror("shm_unlink");


    /* whole memory-segment size */
    err = ftruncate(f, len);
    if (err < 0)
        die("ftruncate");


    /* page1 - memory view onto array page[0] */
    page1 = mmap(/*addr=*/NULL, len,
                PROT_READ | PROT_WRITE,
                MAP_SHARED, // | MAP_HUGETLB | MAP_UNINITIALIZED ?
                f, 0);

    if (page1 == MAP_FAILED)
        die("mmap page1");

    TRACE("mmap page1 ok");

    page1[0] = 1;
    TRACE("store page1 ok (%i)", page1[0]);

    /* page2 - memory view onto array page[0] (content should be identical to page1) */
    page2 = mmap(/*addr=*/NULL, len,
                PROT_READ | PROT_WRITE,
                MAP_SHARED, // | MAP_HUGETLB  | MAP_UNINITIALIZED ?
                f, 0);

    if (page2 == MAP_FAILED)
        die("mmap page2");

    TRACE("mmap page2 ok (%i)", page2[0]);
    assert(page2[0] == 1);


    /* alloc 2*page contiguous VMA */
    page12 = mmap(NULL, 2*len, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (page12 == MAP_FAILED)
        die("mmap page12");

    TRACE("stub page12 ok");

    /* page12[0] -> array.page[0] */
    p = mmap(&page12[0*len], len, PROT_READ, MAP_SHARED | MAP_FIXED, f, 0);
    if (p == MAP_FAILED || (p != &page12[0*len]))
        die("mmap page12.0");

    /* page12[1] -> array.page[0] */
    p = mmap(&page12[1*len], len, PROT_READ, MAP_SHARED | MAP_FIXED, f, 0);
    if (p == MAP_FAILED || (p != &page12[1*len]))
        die("mmap page12.1");

    TRACE("page12 ok (%i %i)", page12[0], page12[len]);
    assert(page12[0]   == 1);
    assert(page12[len] == 1);

    page1[0] = 33;
    TRACE("page12 ok (%i %i)", page12[0], page12[len]);
    assert(page12[0]   == 33);
    assert(page12[len] == 33);

    page2[0] = 45;
    TRACE("page12 ok (%i %i)", page12[0], page12[len]);
    assert(page12[0]   == 45);
    assert(page12[len] == 45);

    /* should segfault - we only requested PROT_READ */
    TRACE("will segfault...");
    page12[0] = 55;

    return 0;
}
