#ifndef REPOSITORY_H
#define REPOSITORY_H 1

#include "cvmfs_config.h"

#include <string>
#include <vector>
#include <sys/stat.h>

#include "hash.h"

struct file_sha1_t {
   std::string path;
   hash::t_sha1 sha1;
   struct stat64 info;

   file_sha1_t(std::string p, struct stat64 i) : path(p), info(i) {}
};


std::string canonical_path(std::string p);
bool is_special_file(std::string name);
void split_path(const std::string &path, std::string &p, std::string &n);

int get_num_threads();
bool compress(std::vector<file_sha1_t> &list);
bool insert_files(std::vector<file_sha1_t> &list);


#endif
