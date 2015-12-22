
#include "cvmfs_config.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <vector>

#include "libcvmfs.h"

static void log_verbose(const char *msg) {
    printf("%s\n", msg);
}


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
            "  -v              Enable verbose mode\n\n",
            PACKAGE_VERSION, argv0, argv0
    );
}


// Copied from util.cc to avoid external dependencies.
// TODO(bbockelm) Revisit - do we really need to avoid all deps?
std::string GetFileName(const std::string &path) {
  const std::string::size_type idx = path.find_last_of('/');
  if (idx != std::string::npos)
    return path.substr(idx+1);
  else
    return path;
}


std::string GetParentPath(const std::string &path) {
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
        if (retval == -1) {
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
                if (-1 == cvmfs_stat(ctx, it->c_str(), &st)) {
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
    return 0;
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
    std::string global_opts;
    std::string repo_opts;
    int c;
    std::string cvmfs_prefix = "/cvmfs/", repository_name, my_repository_name;
    std::vector<std::string> sources;
    size_t slash_idx, next_slash_idx;
    while (-1 != (c = getopt(argc, argv, "g:m:rvi"))) {
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
    if (cvmfs_init(global_opts.c_str())) {
        fprintf(stderr, "Failed to initialize libcvmfs with global options.\n");
        return 2;
    }

    struct stat st;
    if (!ignore_mount && (0 == stat(("/cvmfs/" + repository_name).c_str(), &st))) {
        return real_cp(repository_name, sources, dest, recursive, verbose);
    }

    int retval = libcvmfs_copy_repository_tree(repository_name, repo_opts,
                                               sources, dest, verbose, recursive);

    cvmfs_fini();
    return retval;
}
