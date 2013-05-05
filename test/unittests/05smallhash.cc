// Make:
// g++ -g -O2 -I ../../cvmfs -I ../../externals/murmur/src -o 05smallhash \
//   ../../externals/murmur/src/MurmurHash2.cpp \
//   05smallhash.cc

#include "smallhash.h"
#include "MurmurHash2.h"
#include <cassert>

const int N = 10000000;

uint32_t hasher_int(const int &key) {
  return MurmurHash2(&key, sizeof(key), 0x07387a4f);
}

void PrintStat(const SmallHashDynamic<int, int> &smallhash, const char *prefix)
{
  uint64_t num_collisions;
  uint32_t max_collision;
  smallhash.GetCollisionStats(&num_collisions, &max_collision);
  printf("%s: size: %lu, capacity %lu, collisions %lu/%lu, migrates: %lu\n",
         prefix, smallhash.size(), smallhash.capacity(), num_collisions,
         max_collision, smallhash.num_migrates());
}

int main() {
  SmallHashDynamic<int, int> smallhash;

  smallhash.Init(16, -1, hasher_int);
  for (int i = 0; i < N; ++i) {
    //printf("SIZE BEFORE %lu\n", smallhash.size());
    smallhash.Insert(i, i);
    //printf("SIZE AFTER %lu\n", smallhash.size());
  }
  PrintStat(smallhash, "INSERT");
  for (int i = 0; i < N; ++i) {
    smallhash.Erase(i);
  }
  smallhash.Erase(N+1);
  PrintStat(smallhash, "ERASE");
  for (int i = 0; i < N; ++i) {
    smallhash.Insert(i, i);
  }
  for (int i = 0; i < N; ++i) {
    int value;
    bool found = smallhash.Lookup(i, &value);
    assert(found && (value == i));
  }
  PrintStat(smallhash, "INSERT + LOOKUP");
  smallhash.Clear();
  PrintStat(smallhash, "CLEAR");

  return 0;
}
