/**
 * This file is part of the CernVM File System.
 *
 * Some common functions.
 */

#define _FILE_OFFSET_BITS 64

#include "cvmfs_config.h"
#include "util.h"
#include "hash.h"

#include "platform.h"

#include <string>
#include <sstream>
#include <fstream>
#include <map>
#include <iomanip>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <pthread.h>

extern "C" {
   #include "smalloc.h"
}


using namespace std;  // NOLINT


string canonical_path(const string &p) {
   if (p.length() == 0) return p;

   if (p[p.length()-1] == '/')
      return p.substr(0, p.length()-1);
   else
      return p;
}


string get_parent_path(const string &path) {
   const string::size_type idx = path.find_last_of('/');
   if (idx != string::npos)
      return path.substr(0, idx);
   else
      return "";
}


string get_file_name(const string &path) {
   const string::size_type idx = path.find_last_of('/');
   if (idx != string::npos)
      return path.substr(idx+1);
   else
      return path;
}

bool file_exists(const string &path) {
   platform_stat64 info;
   return ((platform_lstat(path.c_str(), &info) == 0) &&
           S_ISREG(info.st_mode));
}

bool directory_exists(const std::string &path) {
	platform_stat64 info;
	return ((platform_lstat(path.c_str(), &info) == 0) &&
          S_ISDIR(info.st_mode));
}

bool mkdir_deep(const std::string &path, mode_t mode) {
   if (path == "") return false;

   int res = mkdir(path.c_str(), mode);
   if (res == 0) return true;

   if (errno == EEXIST) {
      platform_stat64 info;
      if ((platform_lstat(path.c_str(), &info) == 0) && S_ISDIR(info.st_mode))
         return true;
      return false;
   }

   if ((errno == ENOENT) && (mkdir_deep(get_parent_path(path), mode))) {
      return mkdir(path.c_str(), mode) == 0;
   }

   return false;
}


/**
 * Creates the SHA1 cache directory structure in path.
 */
bool make_cache_dir(const string &path, const mode_t mode) {
   const string cpath = canonical_path(path);

   string lpath = cpath + "/quarantaine";
   if (!mkdir_deep(lpath, mode)) return false;

   lpath = cpath + "/ff";

   platform_stat64 buf;
   if (platform_stat(lpath.c_str(), &buf) != 0) {
      if (mkdir(lpath.c_str(), mode) != 0) return false;
      lpath = cpath + "/txn";
      if (mkdir(lpath.c_str(), mode) != 0) return false;
      for (int i = 0; i < 0xff; i++) {
         char hex[3];
         snprintf(hex, 3, "%02x", i);
         lpath = cpath + "/" + string(hex);
         if (mkdir(lpath.c_str(), mode) != 0) return false;
      }
   }
   return true;
}

/**
 * Converts seconds since UTC 0 into something readable
 */
string localtime_ascii(time_t seconds, const bool utc) {
   struct tm timestamp;
   if (utc) {
      localtime_r(&seconds, &timestamp);
   } else {
      gmtime_r(&seconds, &timestamp);
   }

   const string months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug",
                            "Sep", "Oct", "Nov", "Dec"};
   ostringstream result;
   result << timestamp.tm_mday << " "
          << months[timestamp.tm_mon] << " "
          << timestamp.tm_year + 1900 << " "
          << setw(2) << setfill('0') << timestamp.tm_hour << ":"
          << setw(2) << setfill('0') << timestamp.tm_min << ":"
          << setw(2) << setfill('0') << timestamp.tm_sec;

   return result.str();
}


bool parse_keyval(const string filename, map<char, string> &content) {
   ifstream f;
   f.open(filename.c_str());
   if (!f)
      return false;

   string line;
   while (getline(f, line)) {
      //printf("LINE: %s\n", line.c_str());

      if (line == "--")
         break;
      if (line == "")
         continue;
      const string tail = (line.length() == 1) ? "" : line.substr(1);
      content[line[0]] = tail;
	}

   f.close();
   return true;
}

bool parse_keyval(const char *buf, const int size, int &sig_start,
                  hash::t_sha1 &sha1, map<char, std::string> &content)
{
   istringstream s(string(buf, size));

   string line;
   unsigned pos = 0;
   while (getline(s, line)) {
      pos += line.length()+1;
      //printf("LINE: %s\n", line.c_str());
      if (line == "--")
         break;
      if (line == "")
         continue;
      const string tail = (line.length() == 1) ? "" : line.substr(1);
      content[line[0]] = tail;
	}

   sig_start = pos;
   if (getline(s, line)) {
      //printf("LINE: %s, sigstart %u \n", line.c_str(), pos);
      sha1.from_hash_str(line);
   } else {
      sig_start = -1;
      sha1 = hash::t_sha1();
   }

   return true;
}


/**
 * Reads after skip bytes in memory, looks for a line break and saves
 * the rest into sig_buf, which will be allocated.
 */
bool read_sig_tail(const void *buf, const unsigned buf_size, const unsigned skip,
                   void **sig_buf, unsigned *sig_buf_size)
{
   unsigned i;
   for (i = skip; i < buf_size; ++i) {
      if (((char *)buf)[i] == '\n') break;
   }
   i++;
   /* at least one byte after \n required */
   if (i >= buf_size) {
      *sig_buf = NULL;
      *sig_buf_size = 0;
      return false;
   } else {
      *sig_buf_size = buf_size-i;
      *sig_buf = smalloc(*sig_buf_size);
      memcpy(*sig_buf, ((char *)buf)+i, *sig_buf_size);
      return true;
   }
}


// TODO: Move to compression
bool write_memchunk(const string &path, const void *chunk, const int &size) {
   int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, plain_file_mode);
   if (fd < 0)
      return false;

   int written = write(fd, chunk, size);
   close(fd);

   return written == size;
}


FILE *temp_file(const string &path_prefix, const int mode, const char *open_flags,
                string &final_path) {
   final_path = path_prefix + ".XXXXXX";
   char *tmp_file = strdupa(final_path.c_str());
   int tmp_fd = mkstemp(tmp_file);
   if (tmp_fd < 0)
      return NULL;
   if (fchmod(tmp_fd, mode) != 0) {
      close(tmp_fd);
      return NULL;
   }

   final_path = tmp_file;
   FILE *tmp_fp = fdopen(tmp_fd, open_flags);
   if (!tmp_fp) {
      close(tmp_fd);
      unlink(tmp_file);
      return NULL;
   }

   return tmp_fp;
}


/*
 * Copyright (c) 1997 Shigio Yamaguchi. All rights reserved.
 * Copyright (c) 1999 Tama Communications Corporation. All rights reserved.
 */
char *
abs2rel(const char *path, const char *base, char *result, const size_t size) {
	const char *pp, *bp, *branch;
	/*
	 * endp points the last position which is safe in the result buffer.
	 */
	const char *endp = result + size - 1;
	char *rp;

	if (*path != '/') {
		if (strlen(path) >= size)
			goto erange;
		strcpy(result, path);
		goto finish;
	} else if (*base != '/' || !size) {
		errno = EINVAL;
		return (NULL);
	} else if (size == 1)
		goto erange;
	/*
	 * seek to branched point.
	 */
	branch = path;
	for (pp = path, bp = base; *pp && *bp && *pp == *bp; pp++, bp++)
		if (*pp == '/')
			branch = pp;
	if ((*pp == 0 || (*pp == '/' && *(pp + 1) == 0)) &&
	    (*bp == 0 || (*bp == '/' && *(bp + 1) == 0))) {
		rp = result;
		*rp++ = '.';
		if (*pp == '/' || *(pp - 1) == '/')
			*rp++ = '/';
		if (rp > endp)
			goto erange;
		*rp = 0;
		goto finish;
	}
	if ((*pp == 0 && *bp == '/') || (*pp == '/' && *bp == 0))
		branch = pp;
	/*
	 * up to root.
	 */
	rp = result;
	for (bp = base + (branch - path); *bp; bp++)
		if (*bp == '/' && *(bp + 1) != 0) {
			if (rp + 3 > endp)
				goto erange;
			*rp++ = '.';
			*rp++ = '.';
			*rp++ = '/';
		}
	if (rp > endp)
		goto erange;
	*rp = 0;
	/*
	 * down to leaf.
	 */
	if (*branch) {
		if (rp + strlen(branch + 1) > endp)
			goto erange;
		strcpy(rp, branch + 1);
	} else
		*--rp = 0;
finish:
	return result;
erange:
	errno = ERANGE;
	return (NULL);
}

/*
 * Copyright (c) 1997 Shigio Yamaguchi. All rights reserved.
 * Copyright (c) 1999 Tama Communications Corporation. All rights reserved.
 */
char *
rel2abs(const char *path, const char *base, char *result, const size_t size) {
	const char *pp, *bp;
	/*
	 * endp points the last position which is safe in the result buffer.
	 */
	const char *endp = result + size - 1;
	char *rp;
	int length;

	if (*path == '/') {
		if (strlen(path) >= size)
			goto erange;
		strcpy(result, path);
		goto finish;
	} else if (*base != '/' || !size) {
		errno = EINVAL;
		return (NULL);
	} else if (size == 1)
		goto erange;

	length = strlen(base);

	if (!strcmp(path, ".") || !strcmp(path, "./")) {
		if (length >= (int)size)
			goto erange;
		strcpy(result, base);
		/*
		 * rp points the last char.
		 */
		rp = result + length - 1;
		/*
		 * remove the last '/'.
		 */
		if (*rp == '/') {
			if (length > 1)
				*rp = 0;
		} else
			rp++;
		/* rp point NULL char */
		if (*++path == '/') {
			/*
			 * Append '/' to the tail of path name.
			 */
			*rp++ = '/';
			if (rp > endp)
				goto erange;
			*rp = 0;
		}
		goto finish;
	}
	bp = base + length;
	if (*(bp - 1) == '/')
		--bp;
	/*
	 * up to root.
	 */
	for (pp = path; *pp && *pp == '.'; ) {
		if (!strncmp(pp, "../", 3)) {
			pp += 3;
			while (bp > base && *--bp != '/')
				;
		} else if (!strncmp(pp, "./", 2)) {
			pp += 2;
		} else if (!strncmp(pp, "..\0", 3)) {
			pp += 2;
			while (bp > base && *--bp != '/')
				;
		} else
			break;
	}
	/*
	 * down to leaf.
	 */
	length = bp - base;
	if (length >= (int)size)
		goto erange;
	strncpy(result, base, length);
	rp = result + length;
	if (*pp || *(pp - 1) == '/' || length == 0)
		*rp++ = '/';
	if (rp + strlen(pp) > endp)
		goto erange;
	strcpy(rp, pp);
finish:
	return result;
erange:
	errno = ERANGE;
	return (NULL);
}

bool get_file_info(const string &path, platform_stat64 *info) {
	if (platform_lstat(path.c_str(), info) != 0) {
		stringstream ss;
		ss << "could not stat " << path;
		printWarning(ss.str());
		return false;
	}

	return true;
}

void printError(const string &message) {
	cerr << "[ERROR] " << message << endl;
}
void printWarning(const string &message) {
	cerr << "[WARNING] " << message << endl;
}

// TODO: perhaps in logging
string humanizeBitmap(const unsigned int bitmap) {
   stringstream outp;
   unsigned int mask = 1 << (sizeof(int) * 8 - 1);

	for (int i = sizeof(int) * 8 - 1; i >= 0; --i) {
		int bit = 0;
		if ((bitmap & mask) != 0) bit = 1;
		mask = mask >> 1;
		outp << bit;
	}

   return outp.str();
}

// TODO: merge
vector<string> split_string(const string& s, const string& delim, const bool keep_empty) {
    vector<string> result;
    if (delim.empty()) {
        result.push_back(s);
        return result;
    }
    string::const_iterator substart = s.begin(), subend;
    while (true) {
        subend = search(substart, s.end(), delim.begin(), delim.end());
        string temp(substart, subend);
        if (keep_empty || !temp.empty()) {
            result.push_back(temp);
        }
        if (subend == s.end()) {
            break;
        }
        substart = subend + delim.size();
    }
    return result;
}

vector<string> split_string(const string &str, const char delim) {
  vector<string> result;

  std::stringstream ss(str);
  std::string item;
  while(std::getline(ss, item, delim)) {
      result.push_back(item);
  }

  return result;
}


string join_strings(const vector<string> &strings, const string &joint) {
  string result = "";
  const unsigned size = strings.size();

  if (size > 0) {
    result = strings[0];
    for (unsigned i = 1; i < size; ++i)
      result += joint + strings[i];
  }

  return result;
}


bool HasPrefix(const string &str, const string &prefix,
               const bool ignore_case)
{
  if (prefix.length() > str.length())
    return false;

  for (unsigned i = 0, l = prefix.length(); i < l; ++i) {
    if (ignore_case) {
      if (toupper(str[i]) != toupper(prefix[i]))
        return false;
    } else {
      if (str[i] != prefix[i])
        return false;
    }
  }
  return true;
}


double DiffTimeSeconds(struct timeval start, struct timeval end) {
  // Time substraction, from GCC documentation
  if (end.tv_usec < start.tv_usec) {
    int nsec = (end.tv_usec - start.tv_usec) / 1000000 + 1;
    start.tv_usec -= 1000000 * nsec;
    start.tv_sec += nsec;
  }
  if (end.tv_usec - start.tv_usec > 1000000) {
    int nsec = (end.tv_usec - start.tv_usec) / 1000000;
    start.tv_usec += 1000000 * nsec;
    start.tv_sec -= nsec;
  }

  // Compute the time remaining to wait in microseconds.
  // tv_usec is certainly positive.
  uint64_t elapsed_usec = ((end.tv_sec - start.tv_sec)*1000000) +
                          (end.tv_usec - start.tv_usec);
  return double(elapsed_usec)/1000000.0;
}

