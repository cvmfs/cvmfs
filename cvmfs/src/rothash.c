#include "sha1.h"
#include "compression.h"
#include <stdio.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>

static void printhash(void *uncompr_buf, size_t uncompr_size) {
   void *out_buf;
   size_t out_size;
   unsigned char digest[SHA1_DIGEST_LENGTH];
   char sha1_str[41];
   
   compress_mem(uncompr_buf, uncompr_size, &out_buf, &out_size);
   sha1_mem(out_buf, out_size, digest);
   sha1_string(digest, sha1_str);
   
   free(out_buf);
   
   printf("%s\n", sha1_str);
}

int main(int argc, char **argv) {
   if (argc < 2)
      return 1;
   
   char *file = argv[1];
   FILE *f = fopen (file, "r");
   if (f == NULL)
      return 1;
   
   struct stat info;
   if (fstat(fileno(f), &info) != 0)
      return 1;
   
   char *mem = (char *)malloc(2*info.st_size);
   if (fread(mem+info.st_size, 1, info.st_size, f) != info.st_size)
      return 1;
   
   
   size_t i;
   /*printf("Checking incomplete file\n");
   for (i = 0; i <= info.st_size; ++i) {
      printhash(mem+info.st_size, info.st_size-i);
   }*/
   
   printf("Failed first attempt\n");
   for (i = 1; i <= info.st_size; ++i) {
      memcpy(mem+info.st_size-i, mem+info.st_size, i);
      printhash(mem+info.st_size-i, info.st_size+i);
   }
   
   return 0;
}
