/*
gcc -o fuse_version fuse_version.c `pkg-config fuse3 --cflags --libs`
*/
#define FUSE_USE_VERSION 31

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <assert.h>

// Custom init function to capture and print protocol version
static void *my_init(struct fuse_conn_info *conn, struct fuse_config *cfg) {
    printf("FUSE protocol version negotiated: %u.%u\n", 
           conn->proto_major, conn->proto_minor);
    
    printf("Connection capabilities:\n");
    printf("  Max read size: %u\n", conn->max_read);
    printf("  Max write size: %u\n", conn->max_write);
    printf("  Max readahead: %u\n", conn->max_readahead);
    printf("  Capable flags: 0x%08x\n", conn->capable);
    printf("  Want flags: 0x%08x\n", conn->want);
    
    // You can modify config here if needed
    (void)cfg;  // Suppress unused parameter warning
    
    // Exit immediately after printing protocol version
    fuse_exit(fuse_get_context()->fuse);
    
    return NULL;
}

// Simple operations structure - we only need init
static struct fuse_operations fuse_ops = {
    .init = my_init,
};

int main(int argc, char *argv[]) {
    // Check arguments
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <mountpoint>\n", argv[0]);
        return 1;
    }
    
    printf("Initializing FUSE filesystem...\n");
    
    // Initialize and run FUSE
    // This will call our init function and then immediately exit
    // since we don't implement any other operations
    int ret = fuse_main(argc, argv, &fuse_ops, NULL);
    
    return ret;
}
