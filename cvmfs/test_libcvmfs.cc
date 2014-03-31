/*
This is a simple test program to test the facilities of the
libcvmfs C (not C++) library, which is used by Parrot and some other
tools.

The goal here is not so much to build the ultimate testing tool,
but to provide a simple build target which can verify that libcvmfs
is exporting the proper set of symbols to be used by a C program.
*/

#include "libcvmfs.h"

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <assert.h>

#define TEST_LINE_MAX 1024

void cvmfs_test_help()
{
	printf("commands are:\n");
	printf("   list <path>\n");
	printf("   cat  <path>\n");
	printf("   quit\n");
}

int cvmfs_test_list( const char *path )
{
	char filepath[TEST_LINE_MAX];
	struct stat info;

	char **buffer=0;
	size_t length=0;
	int i;

	int result = cvmfs_listdir(path,&buffer,&length);
	if(result<0) {
		fprintf(stderr,"%s: %s\n",path,strerror(errno));
		return -1;
	}


	for(i=0;buffer[i];i++) {
		snprintf(filepath,TEST_LINE_MAX,"%s/%s",path,buffer[i]);
		cvmfs_stat(filepath,&info);
		printf("%10llu %s\n",(long long unsigned)info.st_size,buffer[i]);
	}

	free(buffer);

	return 0;
}

int cvmfs_test_cat( const char *path )
{
	char buffer[TEST_LINE_MAX];

	int fd = cvmfs_open(path);
	if(fd<0) {
		fprintf(stderr,"%s: %s\n",path,strerror(errno));
		return fd;
	}

	while(1) {
		int length = read(fd,buffer,sizeof(buffer));
		if(length<=0) break;
		int retval = write(1,buffer,length);
    assert(retval == length);
	}

	cvmfs_close(fd);

	return 0;
}

int main( int argc, char *argv[] )
{
	char line[TEST_LINE_MAX];
	char path[TEST_LINE_MAX];

	const char *options = "repo_name=cms.cern.ch,url=http://cvmfs-stratum-one.cern.ch/opt/cms;http://cernvmfs.gridpp.rl.ac.uk/opt/cms;http://cvmfs.racf.bnl.gov/opt/cms,cachedir=test-libcvmfs-cache,alien_cachedir=alien-libcvmfs-cache,pubkey=cern.pubkey";

	printf("%s: initializing with options: %s\n",argv[0],options);

	if(cvmfs_init(options)!=0) {
		fprintf(stderr,"couldn't initialize cvmfs!\n");
		return -1;
	}

	while(1) {

		printf("cvmfs> ");
		fflush(stdout);

		if(!fgets(line,sizeof(line),stdin)) break;

		line[strlen(line)-1] = 0;

		if(sscanf(line,"list %s",path)==1) {
			cvmfs_test_list(path);
		} else if(sscanf(line,"cat %s",path)==1) {
			cvmfs_test_cat(path);
		} else if(!strcmp(line,"quit")) {
			break;
		} else {
			cvmfs_test_help();
		}
	}

	return 0;
}

