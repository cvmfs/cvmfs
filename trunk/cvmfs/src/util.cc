/**
 * \file util.cc
 *
 * Some common functions.
 *
 * Developed by Jakob Blomer 2010 at CERN
 * jakob.blomer@cern.ch
 */
 
#define _FILE_OFFSET_BITS 64

#include "config.h"
#include "util.h"
#include "hash.h"

#include "compat.h"

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

extern "C" {
   #include "smalloc.h"
}


using namespace std;


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

bool is_empty_dir(const string &path) {
   DIR *dir = opendir(path.c_str());
   if (!dir)
      return true;
      
   PortableDirent *d;
   while ((d = portableReaddir(dir)) != NULL) {
      if ((string(d->d_name) == ".") || (string(d->d_name) == "..")) continue;
      closedir(dir);
      return false;
   }
   closedir(dir);
   return true;
}

bool file_exists(const string &path) {
   PortableStat64 info;
   return ((portableFileStat64(path.c_str(), &info) == 0) && S_ISREG(info.st_mode));
}

bool mkdir_deep(const std::string &path, mode_t mode) {
   if (path == "") return false;
   
   int res = mkdir(path.c_str(), mode);
   if (res == 0) return true;
   
   if (errno == EEXIST) {
      PortableStat64 info;
      if ((portableFileStat64(path.c_str(), &info) == 0) && S_ISDIR(info.st_mode))
         return true;
      return false;
   }
   
   if ((errno == ENOENT) && (mkdir_deep(get_parent_path(path), mode))) {
      return mkdir(path.c_str(), mode) == 0;
   }
   
   return false;
}


/**
 * Expands environment variables, i.e. $(homedir)/bla is converted to an absolute path.
 */
string expand_env(const string &path) {
   string result = "";
   
   for (string::size_type i = 0; i < path.length(); i++) {
      string::size_type lpar;
      string::size_type rpar;
      if ((path[i] == '$') && 
          ((lpar = path.find('(', i+1)) != string::npos) &&
          ((rpar = path.find(')', i+2)) != string::npos) &&
          (rpar > lpar))  
      {
         string var = path.substr(lpar + 1, rpar-lpar-1);
         char *var_exp = getenv(var.c_str()); /* Don't free! Nothing is allocated here */
         if (var_exp) {
            result += var_exp;
         }
         i = rpar;
      } else {
         result += path[i];
      }
   }
   
   return result;
}


/**
 * Creates the SHA1 cache directory structure in path.
 */
bool make_cache_dir(const string &path, const mode_t mode) {
   const string cpath = canonical_path(path);
   string lpath = cpath + "/ff";
   PortableStat64 buf;
   if (portableFileStat64(lpath.c_str(), &buf) != 0) {
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



