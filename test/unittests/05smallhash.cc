// Make:
// g++ -g -O2 -I ../../cvmfs -I ../../externals/murmur/src \
//   -I ../../../build/externals/sparsehash/src/src \
//   -o 05smallhash \
//   ../../externals/murmur/src/MurmurHash2.cpp \
//   05smallhash.cc -pthread

#include "smallhash.h"
#include "MurmurHash2.h"
#include <cassert>
#include <time.h>
#include <google/sparse_hash_map>

const int N = 10000000;
const int THREADS = 8;
const int HASHMAPS = 16;
MultiHash<int, int> multihash;

template <typename hashed_type>
struct hash_murmur {
  size_t operator() (const hashed_type key) const {
#ifdef __x86_64__
    return MurmurHash64A(&key, sizeof(key), 0x9ce603115bba659bLLU);
#else
    return MurmurHash2(&key, sizeof(key), 0x07387a4f);
#endif
  }
};

static void *tf_insert(void *data) {
  int ID = (long)data;
  printf("started insert thead %d\n", ID);

  for (int i = (N/THREADS)*ID; i < (N/THREADS)*ID+(N/THREADS); ++i) {
    multihash.Insert(i, i);
  }
  return NULL;
}

static void *tf_erase(void *data) {
  int ID = (long)data;
  printf("started erase thead %d\n", ID);

  for (int i = (N/THREADS)*ID; i < (N/THREADS)*ID+(N/THREADS); ++i) {
    multihash.Erase(i);
  }
  return NULL;
}


uint32_t hasher_int(const int &key) {
  return MurmurHash2(&key, sizeof(key), 0x07387a4f);
}

void PrintStat(const SmallHashDynamic<int, int> &smallhash, const char *prefix)
{
  uint64_t num_collisions;
  uint32_t max_collision;
  smallhash.GetCollisionStats(&num_collisions, &max_collision);
  printf("%s: size: %u, capacity %lu, collisions %lu/%u, migrates: %u\n",
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

  printf("MULTI-HASH:\n");
  uint32_t sizes[HASHMAPS];
  uint32_t overallsize;

  time_t start = time(NULL);
  multihash.Init(HASHMAPS, -1, hasher_int);
  for (int i = 0; i < N; ++i) {
    //printf("SIZE BEFORE %lu\n", smallhash.size());
    multihash.Insert(i, i);
    //printf("SIZE AFTER %lu\n", smallhash.size());
  }
  for (int i = 0; i < N; ++i) {
    multihash.Erase(i);
  }
  for (int i = 0; i < N; ++i) {
    multihash.Insert(i, i);
  }
  for (int i = 0; i < N; ++i) {
    int value;
    bool found = multihash.Lookup(i, &value);
    assert(found && (value == i));
  }
  time_t end = time(NULL);
  //multihash.Clear();
  printf("Real Time: %d seconds\n", end-start);
  multihash.GetSizes(sizes);
  overallsize = 0;
  for (int i = 0; i < HASHMAPS; ++i) {
    printf("Size subtable %d %u\n", i, sizes[i]);
    overallsize += sizes[i];
  }
  printf("overall size: %u\n", overallsize);

  printf("MULTI-HASH MultiThreaded Insert + Erase:\n");
  pthread_t threads[2*THREADS];
  start = time(NULL);
  for (int i = 0; i < 2*THREADS; i++) {
    pthread_create(&threads[i], NULL, tf_insert, (void *)i);
    i++;
    pthread_create(&threads[i], NULL, tf_insert, (void *)i);
  }
  for (int i = 0; i < 2*THREADS; i += 1) {
    pthread_join(threads[i], NULL);
  }
  for (int i = 0; i < 2*THREADS; i++) {
    pthread_create(&threads[i], NULL, tf_erase, (void *)i);
    i++;
    pthread_create(&threads[i], NULL, tf_erase, (void *)i);
  }
  for (int i = 0; i < 2*THREADS; i += 1) {
    pthread_join(threads[i], NULL);
  }
  end = time(NULL);
  printf("Real Time: %d seconds\n", end-start);

  multihash.GetSizes(sizes);
  overallsize = 0;
  for (int i = 0; i < HASHMAPS; ++i) {
    printf("Size subtable %d %u\n", i, sizes[i]);
    overallsize += sizes[i];
  }
  printf("overall size: %u\n", overallsize);


  printf("SPARSEMAP\n");
  google::sparse_hash_map<int, int, hash_murmur<int> > sparsemap;
  sparsemap.set_deleted_key(-1);
  for (int i = 0; i < N; ++i) {
    sparsemap[i] = i;
  }
  start = time(NULL);
  for (int i = 0; i < N; ++i) {
    sparsemap.erase(i);
  }
  end = time(NULL);
  printf("Real Time: %d seconds\n", end-start);

  return 0;
}
