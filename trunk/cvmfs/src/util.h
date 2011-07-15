#ifndef CVMFS_UTIL_H
#define CVMFS_UTIL_H 1

#include "config.h"

#include "hash.h"

#include <string>
#include <map>
#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstdio>

const int plain_file_mode = S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH;
const int plain_dir_mode = S_IXUSR | S_IWUSR | S_IRUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;

std::string canonical_path(const std::string &p);
std::string get_parent_path(const std::string &path);
std::string get_file_name(const std::string &path);
bool is_empty_dir(const std::string &path);
bool file_exists(const std::string &path);
bool mkdir_deep(const std::string &path, mode_t mode);
std::string expand_env(const std::string &path);
bool make_cache_dir(const std::string &path, const mode_t mode);
std::string localtime_ascii(time_t seconds, const bool utc);
bool parse_keyval(const std::string filename, std::map<char, std::string> &content);
bool parse_keyval(const char *buf, const int size, int &sig_start,
                  hash::t_sha1 &sha1, std::map<char, std::string> &content);
bool read_sig_tail(const void *buf, const unsigned buf_size, const unsigned skip, 
                   void **sig_buf, unsigned *sig_buf_size); 
bool write_memchunk(const std::string &patch, const void *chunk, const int &size);
FILE *temp_file(const std::string &path_prefix, const int mode, const char *open_flags,
                std::string &final_path);


#endif
