#ifndef SMALLOC_H
#define SMALLOC_H 1

#include <stdlib.h>

void *smalloc(size_t size);
void *srealloc(void *ptr, size_t size);
void *scalloc(size_t count, size_t size);

#endif
