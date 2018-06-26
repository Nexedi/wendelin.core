// Demo program showing that changes made to one MAP_PRIVATE mapping do not
// propagate to another MAP_PRIVATE mapping even in the same process.
#define _GNU_SOURCE
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
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

int main()
{
	int fd;
	uint8_t *P1, *P2;

	fd = open("map-private-dup.c", O_RDONLY);
	if (fd == -1)
		die("open");

	P1 = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
	if (P1 == MAP_FAILED)
		die("mmap P1");

	printf("P1 orig:    %c %c %c ...\n", P1[0], P1[1], P1[2]);

	P1[0] = 'x';
	P1[1] = 'y';
	P1[2] = 'z';

	printf("P1 changed: %c %c %c ...\n", P1[0], P1[1], P1[2]);

	P2 = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
	if (P2 == MAP_FAILED)
		die("mmap P2");

	printf("P2 orig:    %c %c %c ...\n", P2[0], P2[1], P2[2]);

	// below asserts will fail because P2 does not see changes of P1.
	assert (P2[0] == 'x');
	assert (P2[1] == 'y');
	assert (P2[2] == 'z');

	P1[0] = 99;
	assert (P2[0] == 99);

	printf("OK\n");
	return 0;
}
