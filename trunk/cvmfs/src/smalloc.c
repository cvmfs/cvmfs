
#include "smalloc.h"
#include <stdlib.h>
#include <assert.h>

void *smalloc(size_t size) 
{
   void *mem = malloc(size);
   assert(mem && "Out Of Memory");
   return mem;
}

void *srealloc(void *ptr, size_t size) 
{
   void *mem = realloc(ptr, size);
   assert(mem && "Out Of Memory");
   return mem;
}

void *scalloc(size_t count, size_t size) 
{
   void *mem = calloc(count, size);
   assert(mem && "Out Of Memory");
   return mem;
}

