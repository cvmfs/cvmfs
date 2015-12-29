
#include "cvmfs_config.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>

#include <map>
#include <string>
#include <vector>

#include "libcvmfs.h"

static void log_verbose(const char *msg) {
    printf("%s\n", msg);
}


static std::vector<char> g_temp_directory;
static bool g_cleanup_temp_directory = false;
static bool g_exit_now = false;


static const char *cern_key_text =
"-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAukBusmYyFW8KJxVMmeCj\n\
N7vcU1mERMpDhPTa5PgFROSViiwbUsbtpP9CvfxB/KU1gggdbtWOTZVTQqA3b+p8\n\
g5Vve3/rdnN5ZEquxeEfIG6iEZta9Zei5mZMeuK+DPdyjtvN1wP0982ppbZzKRBu\n\
BbzR4YdrwwWXXNZH65zZuUISDJB4my4XRoVclrN5aGVz4PjmIZFlOJ+ytKsMlegW\n\
SNDwZO9z/YtBFil/Ca8FJhRPFMKdvxK+ezgq+OQWAerVNX7fArMC+4Ya5pF3ASr6\n\
3mlvIsBpejCUBygV4N2pxIcPJu/ZDaikmVvdPTNOTZlIFMf4zIP/YHegQSJmOyVp\n\
HQIDAQAB\n\
-----END PUBLIC KEY-----\n\
";
static const char *cern_url =
"http://cvmfs-stratum-one.cern.ch/cvmfs/@fqrn@;"
"http://cernvmfs.gridpp.rl.ac.uk/cvmfs/@fqrn@;"
"http://cvmfs.racf.bnl.gov/cvmfs/@fqrn@;"
"http://cvmfs.fnal.gov/cvmfs/@fqrn@;"
"http://cvmfs02.grid.sinica.edu.tw/cvmfs/@fqrn@";


static const char *oasis_key_text =
"-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqQGYXTp9cRcMbGeDoijB\n\
gKNTCEpIWB7XcqIHVXJjfxEkycQXMyZkB7O0CvV3UmmY2K7CQqTnd9ddcApn7BqQ\n\
/7QGP0H1jfXLfqVdwnhyjIHxmV2x8GIHRHFA0wE+DadQwoi1G0k0SNxOVS5qbdeV\n\
yiyKsoU4JSqy5l2tK3K/RJE4htSruPCrRCK3xcN5nBeZK5gZd+/ufPIG+hd78kjQ\n\
Dy3YQXwmEPm7kAZwIsEbMa0PNkp85IDkdR1GpvRvDMCRmUaRHrQUPBwPIjs0akL+\n\
qoTxJs9k6quV0g3Wd8z65s/k5mEZ+AnHHI0+0CL3y80wnuLSBYmw05YBtKyoa1Fb\n\
FQIDAQAB\n\
-----END PUBLIC KEY-----\n\
";
static const char *oasis_url =
"http://cvmfs-egi.gridpp.rl.ac.uk:8000/cvmfs/@fqrn@;"
"http://klei.nikhef.nl:8000/cvmfs/@fqrn@;"
"http://cvmfs-s1bnl.opensciencegrid.org:8000/cvmfs/@fqrn@;"
"http://cvmfs-s1fnal.opensciencegrid.org:8000/cvmfs/@fqrn@;"
"http://cvmfsrep.grid.sinica.edu.tw:8000/cvmfs/@fqrn@";


static const char *egi_key_text =
"-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxKhc7s1HmmPWH4Cq1U3K\n\
4FNFKcMQgZxUrgQEfvgkF97OZ8I8wzC9MWqmegX6tqlPmAzYWTM+Xi4nEBWYRhd+\n\
hVN/prHyYGzb/kTyCSHa9EQtIk9SUyoPfQxkGRnx68pD5con8KJySNa8neplsXx+\n\
2gypwjasBRQLzB3BrrGhrzZ5fL84+dsxNBBW6QfNO1BS5ATeWl3g1J27f0GoGtRO\n\
YbPhaAd9D+B+qVo9pt3jKXvjTZQG0pE16xaX1elciFT9OhtZGaErDJyURskD7g3/\n\
NotcpBL5K5v95zA/kh5u+TRrmeTxHyDOpyrGrkqRaT5p+/C1z0HDyKFQbptegCbn\n\
GwIDAQAB\n\
-----END PUBLIC KEY-----\n\
";
static const char *egi_url =
"http://cvmfs-egi.gridpp.rl.ac.uk:8000/cvmfs/@fqrn@;"
"http://klei.nikhef.nl:8000/cvmfs/@fqrn@;"
"http://cvmfsrepo.lcg.triumf.ca:8000/cvmfs/@fqrn@;"
"http://cvmfsrep.grid.sinica.edu.tw:8000/cvmfs/@fqrn@";


static void usage(const char *argv0) {
   fprintf(stderr,
            "CernVM-FS version %s\n"
            "Copyright (c) 2009-2015 CERN\n"
            "All rights reserved\n\n"
            "Please visit http://cernvm.cern.ch/project/info for license details and author list.\n\n"

            "%s - utility for copying files out of CVMFS\n\n"

            "Usage: %s [-g global_opts] [-m repo_opts] [-r] [-v] src_path dest_path\n\n"

            "Where:\n"
            "  -g global_opts  Global CVMFS options passed to CVMFS.\n"
            "  -m repo_opts    Per-repo options.\n"
            "  -r              Enable recursive mode\n"
            "  -b              Allow use of built-in keys (which may have been revoked).\n"
            "  -v              Enable verbose mode\n\n",
            PACKAGE_VERSION, argv0, argv0
    );
}


std::map<std::string, std::string> parse_opts(char const *options) {
    std::map<std::string, std::string> opts_map;
    while (*options) {
      char const *next = options;
      std::string name;
      std::string value;

      // get the option name
      for (next=options; *next && *next != ',' && *next != '='; next++) {
        if (*next == '\\') {
          next++;
          if (*next == '\0') break;
        }
        name += *next;
      }

      if (*next == '=') {
        next++;
      }

      // get the option value
      for (; *next && *next != ','; next++) {
        if (*next == '\\') {
          next++;
          if (*next == '\0') break;
        }
        value += *next;
      }

      if (!name.empty() || !value.empty()) {
        opts_map[name] = value;
      }

      if (*next == ',') next++;
      options = next;
    }
    return opts_map;
}


static std::string map_to_string(const std::map<std::string, std::string> &opts_map) {
    std::string opts_string;
    bool first_string = true;
    for (std::map<std::string, std::string>::const_iterator it = opts_map.begin();
                                                            it != opts_map.end();
                                                            it++) {
        if (!first_string) {opts_string += ",";}
        first_string = false;

        opts_string += it->first + "=";

        std::string value = it->second;
        size_t escape_val = value.find('\\', 0);
        while (escape_val != std::string::npos) {
            value.replace(escape_val, 1, "\\\\", 2);
            escape_val = value.find('\\', escape_val + 2);
        }
        escape_val = value.find(',', 0);
        while (escape_val != std::string::npos) {
            value.replace(escape_val, 1, "\\,", 2);
            escape_val = value.find(',', escape_val + 2);
        }
        opts_string += value;
    }
    return opts_string;
}


static void start_cleanup(int sig) {
    g_exit_now = true;
    signal(sig, SIG_DFL);
}


static bool make_temp_directory() {
    if (g_cleanup_temp_directory) {return true;}

    g_temp_directory.reserve(50);
    strncpy(&g_temp_directory[0], "/tmp/cvmfs_cp_XXXXXX", 50);
    if (!mkdtemp(&g_temp_directory[0])) {
        fprintf(stderr, "Failed to make temporary directory (%s): %s\n",
                &g_temp_directory[0], strerror(errno));
        return false;
    }
    signal(SIGINT, start_cleanup);
    signal(SIGTERM, start_cleanup);
    signal(SIGQUIT, start_cleanup);
    signal(SIGHUP, start_cleanup);
    g_cleanup_temp_directory = true;

    return true;
}


static void cleanup_temp_directory() {
    static const char *rm = "rm";
    static const char *rf = "-rf";
    std::vector<const char *> exec_info;
    exec_info.reserve(4);
    exec_info[0] = rm;
    exec_info[1] = rf;
    exec_info[2] = &g_temp_directory[0];
    exec_info[3] = NULL;

    pid_t child_pid = fork();
    if (-1 == child_pid) {
        fprintf(stderr, "Failed to fork process to clean temporary directory (%s): %s\n",
                &g_temp_directory[0], strerror(errno));
        return;
    } else if (!child_pid) {
        execvp("/bin/rm", const_cast<char * const*>(&exec_info[0]));
        fprintf(stderr, "Failed to exec process to clean temporary directory (%s): %s\n",
                &g_temp_directory[0], strerror(errno));
        _exit(1);
    }
    int status;
    if (-1 == waitpid(child_pid, &status, 0)) {
        if (WIFEXITED(status)) {
            fprintf(stderr, "Failure to cleanup temporary directory (%s); rm exited with %d.\n",
                    &g_temp_directory[0], WEXITSTATUS(status));
        } else if (WIFSIGNALED(status)) {
            fprintf(stderr, "Failure to cleanup temporary directory (%s); rm exited with signal %d.\n",
                    &g_temp_directory[0], WTERMSIG(status));
        } else {
            fprintf(stderr, "Failure to cleanup temporary directory (%s); rm status code %d.\n",
                    &g_temp_directory[0], status);
        }
    }
}


static std::string write_pubkey(const char *text) {
    if (!make_temp_directory()) {return "";}

    std::string pubkey_location = &g_temp_directory[0];
    pubkey_location += "/pubkey";
    int fd = open(pubkey_location.c_str(), O_CREAT|O_EXCL|O_WRONLY, 0600);
    if (-1 == fd) {
        fprintf(stderr, "Failed to open pubkey file (%s): %s\n",
                pubkey_location.c_str(), strerror(errno));
        return "";
    }
    const char *buf = text;
    size_t bytes_to_write = strlen(text);
    while (bytes_to_write) {
        ssize_t bytes_written = write(fd, buf, bytes_to_write);
        if (-1 == bytes_written) {
            if (errno == EINTR) {continue;}
            fprintf(stderr, "Failed to write to pubkey file (%s): %s\n",
                    pubkey_location.c_str(), strerror(errno));
            close(fd);
            return "";
        }
        bytes_to_write -= bytes_written;
        buf += bytes_written;
    }
    close(fd);
    return pubkey_location;
}


static std::string fill_global_opts(std::string input_opts) {
    std::map<std::string, std::string> opts_map = parse_opts(input_opts.c_str());
    if ((opts_map.find("cachedir") == opts_map.end()) &&
        (opts_map.find("cache_directory") == opts_map.end())) {
       if (!make_temp_directory()) {return "";}
       opts_map["cachedir"] = &g_temp_directory[0];
    }
    return map_to_string(opts_map);
}


static std::string fill_repo_opts(std::string repo_name, std::string input_opts, bool enable_builtin) {
    std::map<std::string, std::string> opts_map = parse_opts(input_opts.c_str());
    if (opts_map.find("proxies") == opts_map.end()) {
        const char *http_proxy = getenv("http_proxy");
        if (http_proxy) {
            std::string http_proxy_str = http_proxy;
            http_proxy_str += ";DIRECT";
            opts_map["proxies"] = http_proxy_str;
        } else {
            opts_map["proxies"] = "DIRECT";
        }
    }
    if ((opts_map.find("pubkey") == opts_map.end()) && enable_builtin) {
        if (repo_name.rfind(".opensciencegrid.org") != std::string::npos) {
            opts_map["pubkey"] = write_pubkey(oasis_key_text);
        } else if (repo_name.rfind(".egi.eu") != std::string::npos) {
            opts_map["pubkey"] = write_pubkey(egi_key_text);
        } else if (repo_name.rfind(".cern.ch") != std::string::npos) {
            opts_map["pubkey"] = write_pubkey(cern_key_text);
        }
    }
    if (opts_map.find("url") == opts_map.end()) {
        if (repo_name.rfind(".opensciencegrid.org") != std::string::npos) {
            opts_map["url"] = oasis_url;
        } else if (repo_name.rfind(".egi.eu") != std::string::npos) {
            opts_map["url"] = egi_url;
        } else if (repo_name.rfind(".cern.ch") != std::string::npos) {
            opts_map["url"] = cern_url;
        }
    }
    std::string url = opts_map["url"];
    size_t escape_val = url.find("@fqrn@");
    while (escape_val != std::string::npos) {
        url.replace(escape_val, 6, repo_name);
        escape_val = url.find("@fqrn@", escape_val + 6);
    }
    opts_map["url"] = url;
    
    return map_to_string(opts_map);
}


// Copied from util.cc to avoid external dependencies.
// TODO(bbockelm) Revisit - do we really need to avoid all deps?
static std::string GetFileName(const std::string &path) {
  const std::string::size_type idx = path.find_last_of('/');
  if (idx != std::string::npos)
    return path.substr(idx+1);
  else
    return path;
}


static std::string GetParentPath(const std::string &path) {
  const std::string::size_type idx = path.find_last_of('/');
  if (idx != std::string::npos)
    return path.substr(0, idx);
  else
    return "";
}


static int libcvmfs_copy_file(cvmfs_context *ctx,
                              const std::string &source,
                              const std::string &dest,
                              const bool verbose) {
    if (g_exit_now) {errno = EIO; return -1;}
    int source_fd = cvmfs_open(ctx, source.c_str());
    if (-1 == source_fd) {
        fprintf(stderr, "Failed to open repository file %s (%s).\n",
                        source.c_str(), strerror(errno));
        return -1;
    }
    struct stat st;
    if (-1 == cvmfs_stat(ctx, source.c_str(), &st)) {
        fprintf(stderr, "Failed to stat repository file %s (%s).\n",
                        source.c_str(), strerror(errno));
        cvmfs_close(ctx, source_fd);
        return -1;
    }
    std::string mydest = dest;
    int dest_fd = open(mydest.c_str(), O_CREAT|O_WRONLY, st.st_mode);
    if ((-1 == dest_fd) && (errno == EISDIR)) {
        std::string source_fname = GetFileName(source);
        mydest = dest + "/" + source_fname;
        dest_fd = open(mydest.c_str(), O_CREAT|O_WRONLY,
                       st.st_mode);
    }
    if (-1 == dest_fd) {
        fprintf(stderr, "Failed to open destination file %s (%s)\n",
                        mydest.c_str(), strerror(errno));
        cvmfs_close(ctx, source_fd);
        return -1;
    }
    if (verbose) {printf("Copying file %s -> %s.\n", source.c_str(),
                         mydest.c_str());}

    const size_t buffer_size = 16*1024;
    std::vector<char> buffer; buffer.reserve(buffer_size);
    off_t offset = 0;
    while (true) {
        int retval = cvmfs_pread(ctx, source_fd, &buffer[0], buffer_size,
                                 offset);
        if (g_exit_now || (retval == -1)) {
            if (g_exit_now) {errno = EIO;}
            if (errno == EINTR) {continue;}
            fprintf(stderr, "Failed to read source file %s (%s)\n",
                            source.c_str(), strerror(errno));
            cvmfs_close(ctx, source_fd);
            close(dest_fd);
            return -1;
        }
        if (retval == 0) {break;} // Success - we've run out of bytes.
        offset += retval;
        size_t bytes_to_write = retval;
        off_t buffer_offset = 0;
        while (bytes_to_write) {
            int dest_retval = write(dest_fd, &buffer[buffer_offset],
                                    bytes_to_write);
            if (-1 == dest_retval) {
                if (errno == EINTR) {continue;}
                fprintf(stderr, "Failed to write to destination file %s (%s)\n",
                                mydest.c_str(), strerror(errno));
                cvmfs_close(ctx, source_fd);
                close(dest_fd);
                return -1;
            }
            buffer_offset += dest_retval;
            bytes_to_write -= dest_retval;
        }
    }
    cvmfs_close(ctx, source_fd);
    close(dest_fd);
    return 0;
}


static int libcvmfs_copy_tree(cvmfs_context *ctx,
                              const std::string &source, 
                              const std::string &dest,
                              const bool verbose) {
    std::vector<std::string> worklist;
    worklist.push_back(source);
    std::string base_dir = GetParentPath(source);
    size_t base_len = base_dir.length();
    while (!worklist.empty()) {
        if (g_exit_now) {errno=EIO; return -1;}
        std::string back = worklist.back();
        worklist.pop_back();
        struct stat st;
        if (-1 == cvmfs_lstat(ctx, back.c_str(), &st)) {
            fprintf(stderr, "Failed to stat source %s (%s)\n",
                            back.c_str(), strerror(errno));
            return -1;
        }
        if (S_ISDIR(st.st_mode)) {
            std::string new_dir = dest + "/" + back.substr(base_len);
            if (verbose) {printf("Copying directory %s -> %s.\n", back.c_str(),
                                 new_dir.c_str());}
            if ((-1 == mkdir(new_dir.c_str(), st.st_mode)) && (errno != EEXIST)) {
                fprintf(stderr, "Failed to make directory %s (%s)\n",
                                new_dir.c_str(), strerror(errno));
                return -1;
            }
            char **entries = NULL;
            size_t entry_len = 0;
            if (-1 == cvmfs_listdir(ctx, back.c_str(), &entries, &entry_len)) {
                fprintf(stderr, "Failed to list directory %s (%s)\n",
                                back.c_str(), strerror(errno));
                return -1;
            }
            for (size_t idx = 0; idx < entry_len; idx++) {
                if (!entries[idx]) {break;}
                std::string fname = entries[idx];
                if ((fname != ".") && (fname != "..")) {
                    std::string full_fname = back + "/" + fname;
                    worklist.push_back(full_fname);
                }
                free(entries[idx]);
            }
            free(entries);
        } else if (S_ISLNK(st.st_mode)) {
            char link_val[PATH_MAX];
            std::string new_link = dest + "/" + back.substr(base_len);
            if (verbose) {printf("Creating link %s.\n", new_link.c_str());} 
            if (-1 == cvmfs_readlink(ctx, back.c_str(), link_val, PATH_MAX)) {
                fprintf(stderr, "Failed to read source (%s) link (%s)\n",
                        back.c_str(), strerror(errno));
                return -1;
            }
            if (strlen(link_val) == 0) {
                fprintf(stderr, "Ignoring invalid empty symlink %s.\n",
                        back.c_str());
                continue;
            }
            int retval = symlink(link_val, new_link.c_str());
            if (-1 == retval) {
                if (errno == EEXIST) {
                    unlink(new_link.c_str());
                    retval = symlink(link_val, new_link.c_str());
                }
                if (-1 == retval) {
                    fprintf(stderr, "Failed to write link %s (%s)\n",
                            new_link.c_str(), strerror(errno));
                    return -1;
                }
            }
        } else if (S_ISREG(st.st_mode)) {
            std::string new_file = dest + "/" + back.substr(base_len);
            if (libcvmfs_copy_file(ctx, back, new_file, verbose)) {
                return -1;
            }
        } else {
            fprintf(stderr, "Warning: ignoring invalid file type %s.\n",
                            back.c_str());
        }
    }
    return 0;
}


static
int libcvmfs_copy_repository_tree(const std::string &repository_name,
                                  const std::string &repository_opts,
                                  const std::vector<std::string> &sources,
                                  const std::string &dest,
                                  const bool verbose,
                                  const bool recursive) {
    std::string full_opts = "repo_name=" + repository_name + "," + repository_opts;
    cvmfs_context *ctx = cvmfs_attach_repo(full_opts.c_str());
    if (ctx == NULL) {
        fprintf(stderr, "Failed to initialize repository %s.\n", repository_name.c_str());
        return 3;
    }

    int retval = 0;
    if (sources.size() == 1) {
        struct stat st;
        if (-1 == cvmfs_stat(ctx, sources[0].c_str(), &st)) {
            fprintf(stderr, "Failed to stat source %s (%s)\n",
                    sources[0].c_str(), strerror(errno));
        }
        if (S_ISDIR(st.st_mode)) {
            if (recursive) {
                retval = libcvmfs_copy_tree(ctx, sources[0], dest, verbose);
            } else {
                fprintf(stderr, "Omitting directory %s\n", sources[0].c_str());
            }
        } else {
            retval = libcvmfs_copy_file(ctx, sources[0], dest, verbose);
        }
    } else {
        struct stat st;
        if (-1 == stat(dest.c_str(), &st)) {
            fprintf(stderr, "Destination (%s) error: %s\n", dest.c_str(),
                            strerror(errno));
            retval = 3;
        } else if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "If multiple sources are specified, destination "
                            "(%s) must be a directory.\n", dest.c_str());
            retval = 3;
        } else {
            for (std::vector<std::string>::const_iterator it = sources.begin();
                 it != sources.end(); it++) {
                if (g_exit_now) {
                    retval = 4;
                } else if (-1 == cvmfs_stat(ctx, it->c_str(), &st)) {
                    fprintf(stderr, "Source (%s) stat error: %s\n", dest.c_str(), 
                            strerror(errno));
                    retval = 3;
                } else if (S_ISDIR(st.st_mode)) {
                    if (recursive) {
                        retval = libcvmfs_copy_tree(ctx, *it, dest, verbose);
                    } else {
                        fprintf(stderr, "Omitting directory %s\n", it->c_str());
                    }
                } else {
                    retval = libcvmfs_copy_file(ctx, *it, dest, verbose);
                }
                if (retval) {break;}
            }
        }
    }

    cvmfs_detach_repo(ctx);
    return retval;
}


static
int real_cp(const std::string repo_name,
            const std::vector<std::string> &sources, const std::string &dest,
            bool recursive, bool verbose) {
    std::vector<const char*> args; args.reserve(sources.size() + 4);
    const std::string cp_name = "cp";
    const std::string recurse_opt = "-r";
    const std::string verbose_opt = "-v";
    args.push_back(cp_name.c_str());
    if (recursive) {
        args.push_back(recurse_opt.c_str());
    }
    if (verbose) {
        args.push_back(verbose_opt.c_str());
    }
    std::vector<std::string> full_sources;
    for (std::vector<std::string>::const_iterator it = sources.begin();
         it != sources.end(); it++) {
        full_sources.push_back("/cvmfs/" + repo_name + "/" + it->c_str());
    }
    for (std::vector<std::string>::const_iterator it = full_sources.begin();
         it != full_sources.end(); it++) {
        args.push_back(it->c_str());
    }
    args.push_back(dest.c_str());
    args.push_back(NULL);
    return execvp("/bin/cp", const_cast<char* const*>(&args[0]));
}


int main(int argc, char *argv[]) {
    bool recursive = false;
    bool verbose = false;
    bool ignore_mount = false;
    bool enable_builtin = false;
    std::string global_opts;
    std::string repo_opts;
    int c;
    std::string cvmfs_prefix = "/cvmfs/", repository_name, my_repository_name;
    std::vector<std::string> sources;
    size_t slash_idx, next_slash_idx;
    while (-1 != (c = getopt(argc, argv, "bg:m:rvi"))) {
        switch (c) {
            case 'g':
                global_opts = optarg;
                break;
            case 'm':
                repo_opts = optarg;
                break;
            case 'r':
                recursive = true;
                break;
            case 'v':
                verbose = true;
                break;
            case 'i':
                ignore_mount = true;
                break;
            case 'b':
                enable_builtin = true;
                break;
            case '?':
                if ((optopt == 'g') || (optopt == 'm')) {
                    usage(argv[0]);
                    fprintf(stderr, "Option -%c requires an argument.\n", optopt);
                } else if (isprint(optopt)) {
                    usage(argv[0]);
                    fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                } else {
                    usage(argv[0]);
                    fprintf(stderr,
                        "Unknown option character `\\x%x'.\n",
                        optopt);
                }
                return 1;
        }
    }

    if (verbose) {
        printf("Global opts: %s\n", global_opts.c_str());
        printf("Repo opts: %s\n", repo_opts.c_str());
        printf("Recursive: %d\n", recursive);
    }

    if (argc-optind < 2) {
        usage(argv[0]);
        fprintf(stderr, "Must at least provide a source and destination.\n");
        return 1;
    }
    for (int index = optind; index < argc-1; index++) {
        std::string arg = argv[index];
        if (arg.compare(0, cvmfs_prefix.length(), cvmfs_prefix)) {
            fprintf(stderr, "Source file %s does not start with %s.\n",
                    argv[index], cvmfs_prefix.c_str());
            return 1;
        }
        slash_idx = cvmfs_prefix.length();
        while ((arg.length() > slash_idx) && (arg[slash_idx] == '/')) {slash_idx++;}
        next_slash_idx = arg.find('/', slash_idx);
        if (next_slash_idx == std::string::npos) {
            fprintf(stderr, "Source %s not of the form /cvmfs/repository/path\n", argv[index]);
            return 1;
        }
        my_repository_name = arg.substr(slash_idx, (next_slash_idx-slash_idx));
        if (repository_name.empty()) {
            repository_name = my_repository_name;
            if (verbose) {
                printf("Using repository %s.\n", repository_name.c_str());
            }
        } else if (my_repository_name != repository_name) {
            fprintf(stderr, "Source %s repository name (%s) does not match "
                            "previous repository (%s); only one repository "
                            "may be used at a time.\n", argv[index],
                            my_repository_name.c_str(),
                            repository_name.c_str());
            return 1;
        }
        arg = arg.substr(next_slash_idx);
        sources.push_back(arg);
    }
    std::string dest = argv[argc-1];
    if (verbose) {printf("Destination %s\n", argv[argc-1]);}

    if (verbose) {
        printf("Initializing libcvmfs with global options.\n");
        cvmfs_set_log_fn(log_verbose);
    }
    global_opts = fill_global_opts(global_opts);
    if (verbose) {printf("Using global options: %s\n", global_opts.c_str());}

    if (cvmfs_init(global_opts.c_str())) {
        fprintf(stderr, "Failed to initialize libcvmfs with global options.\n");
        return 2;
    }

    struct stat st;
    if (!ignore_mount && (0 == stat(("/cvmfs/" + repository_name).c_str(), &st))) {
        if (g_cleanup_temp_directory) {
            cleanup_temp_directory();
        }
        return real_cp(repository_name, sources, dest, recursive, verbose);
    }

    repo_opts = fill_repo_opts(repository_name, repo_opts, enable_builtin);
    if (verbose) {printf("Using repo options: %s\n", repo_opts.c_str());}

    int retval = libcvmfs_copy_repository_tree(repository_name, repo_opts,
                                               sources, dest, verbose, recursive);

    cvmfs_fini();

    if (g_cleanup_temp_directory) {
        cleanup_temp_directory();
    }
    return retval;
}
