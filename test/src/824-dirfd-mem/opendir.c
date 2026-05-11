#include <assert.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <limits.h>
#include <stdio.h>
#include <stdbool.h>

int main(int argc, char *argv[]) {
	assert(argc == 2);
	DIR *dir = opendir(argv[1]);
	if (!dir)
		return 1;
	printf("opened\n");
	fflush(stdout);
	while (true) {
		sleep(INT_MAX);
	}
	return 0;
}
