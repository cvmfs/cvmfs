/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_H_
#define CVMFS_UTIL_H_

#include "cvmfs_config.h"

#include <sys/time.h>
#include <sys/stat.h>
#include <time.h>
#include <fcntl.h>

#include <cstdio>
#include <string>
#include <map>
#include <vector>

#include "platform.h"
#include "hash.h"

const int plain_file_mode = S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH;
const int plain_dir_mode = S_IXUSR | S_IWUSR | S_IRUSR |
                           S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;

std::string canonical_path(const std::string &p);
std::string get_parent_path(const std::string &path);
std::string get_file_name(const std::string &path);
bool file_exists(const std::string &path);
bool directory_exists(const std::string &path);
bool mkdir_deep(const std::string &path, mode_t mode);
bool make_cache_dir(const std::string &path, const mode_t mode);
std::string localtime_ascii(time_t seconds, const bool utc);
bool parse_keyval(const std::string filename, std::map<char, std::string> &content);
bool parse_keyval(const char *buf, const int size, int &sig_start,
                  hash::Any &hash, std::map<char, std::string> &content);
bool read_sig_tail(const void *buf, const unsigned buf_size, const unsigned skip,
                   void **sig_buf, unsigned *sig_buf_size);
bool write_memchunk(const std::string &patch, const void *chunk, const int &size);
FILE *temp_file(const std::string &path_prefix, const int mode, const char *open_flags,
                std::string &final_path);
bool get_file_info(const std::string &path, platform_stat64 *info);

void printError(const std::string &message);
void printWarning(const std::string &message);
std::string humanizeBitmap(const unsigned int bitmap);

/*
 * abs2rel: convert an absolute path name into relative.
 *
 *	i)	path	absolute path
 *	i)	base	base directory (must be absolute path)
 *	o)	result	result buffer
 *	i)	size	size of result buffer
 *	r)		!= NULL: relative path
 *			== NULL: error
 */
char *abs2rel(const char *path, const char *base, char *result, const size_t size);

/*
 * rel2abs: convert an relative path name into absolute.
 *
 *	i)	path	relative path
 *	i)	base	base directory (must be absolute path)
 *	o)	result	result buffer
 *	i)	size	size of result buffer
 *	r)		!= NULL: absolute path
 *			== NULL: error
 */
char *rel2abs(const char *path, const char *base, char *result, const size_t size);

std::vector<std::string> split_string(const std::string& s, const std::string& delim, const bool keep_empty = true);

std::vector<std::string> split_string(const std::string &str, const char delim);
std::string join_strings(const std::vector<std::string> &strings,
                         const std::string &joint);

bool HasPrefix(const std::string &str, const std::string &prefix,
               const bool ignore_case);

double DiffTimeSeconds(struct timeval start, struct timeval end);

#endif  // CVMFS_UTIL_H_
