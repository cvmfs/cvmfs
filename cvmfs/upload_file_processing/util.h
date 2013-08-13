#ifndef UTIL_H
#define UTIL_H

#include <sys/resource.h>
#include <pthread.h>
#include <string.h>
#include <string>

#include <iostream> // TODO: remove

//static const std::string input_path =  "/Volumes/ramdisk/input/onefile";
static const std::string input_path =  "../../big_benchmark";
static const std::string output_path = "/Volumes/ramdisk/output";

static pthread_mutex_t mutex;

static void Print(const std::string &msg) {
  pthread_mutex_lock(&mutex);
  std::cout << msg << std::endl;
  pthread_mutex_unlock(&mutex);
}


static void PrintErr(const std::string &msg) {
  pthread_mutex_lock(&mutex);
  std::cerr << msg << std::endl;
  pthread_mutex_unlock(&mutex);
}


static std::string ShaToString(const unsigned char *sha_digest) {
  char sha_string[41];
  for (unsigned i = 0; i < 20; ++i) {
    char dgt1 = (unsigned)sha_digest[i] / 16;
    char dgt2 = (unsigned)sha_digest[i] % 16;
    dgt1 += (dgt1 <= 9) ? '0' : 'a' - 10;
    dgt2 += (dgt2 <= 9) ? '0' : 'a' - 10;
    sha_string[i*2] = dgt1;
    sha_string[i*2+1] = dgt2;
  }
  sha_string[40] = '\0';
  return std::string(sha_string);
}


static bool RaiseFileDescriptorLimit(unsigned int limit) {
  struct rlimit rpl;
  memset(&rpl, 0, sizeof(rpl));
  getrlimit(RLIMIT_NOFILE, &rpl);
  if (rpl.rlim_cur < limit) {
    if (rpl.rlim_max < limit)
      rpl.rlim_max = limit;
    rpl.rlim_cur = limit;
    const bool retval = setrlimit(RLIMIT_NOFILE, &rpl);
    if (retval != 0) {
      return false;
    }
  }
  return true;
}

#endif /* UTIL_H */
