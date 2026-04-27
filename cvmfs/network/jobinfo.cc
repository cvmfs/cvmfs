/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <sys/stat.h>

#include "jobinfo.h"
#include "util/capabilities.h"
#include "util/logging.h"

#include <inttypes.h>

#include "cache.h"
#include "compression/decompressor_guess.h"
#include "util/string.h"

namespace download {

atomic_int64 JobInfo::next_uuid = 0;

JobInfo::JobInfo()
{
  Init();
  SetDecompressor(zip::Algorithm::kNoCompression);
}

JobInfo::JobInfo(const std::string *u, zip::DecompressionAlg decompressor_alg,
                 const bool ph, const shash::Any *h, cvmfs::Sink *s) {
  Init();
  SetDecompressor(decompressor_alg);

  url_ = u;
  probe_hosts_ = ph;
  head_request_ = false;
  expected_hash_ = h;
  sink_ = s;
}

JobInfo::JobInfo(const std::string* u, zip::Decompressor* decomp, const bool ph,
                 const shash::Any* h, cvmfs::Sink* s)
{
  Init();
  SetDecompressor(decomp);

  url_ = u;
  probe_hosts_ = ph;
  head_request_ = false;
  expected_hash_ = h;
  sink_ = s;
}

JobInfo::JobInfo(const std::string *u, const bool ph)
{
  Init();
  SetDecompressor(zip::kNoCompression);

  url_ = u;
  probe_hosts_ = ph;
  head_request_ = true;
  expected_hash_ = NULL;
  sink_ = NULL;
}


bool JobInfo::IsFileNotFound() {
  if (HasPrefix(*url_, "file://", true /* ignore_case */))
    return error_code_ == kFailHostConnection;

  return http_code_ == 404;
}

void JobInfo::SetDecompressor(zip::Algorithm decompressor_alg) {
  decomp_ = zip::Decompressor::Construct(decompressor_alg);
}

void JobInfo::SetDecompressor(zip::Decompressor* decomp) {
  decomp_ = decomp;
}

void JobInfo::SetDecompressor(const CacheManager::Label &label) {
  if (label.zip_algorithm == zip::Algorithm::kGuessDecompression) {
    decomp_ = new zip::GuessDecompressor(label);
  } else {
    SetDecompressor(label.zip_algorithm);
  }
}

bool JobInfo::ResetDecompression() {
  return decomp_->Reset();
}

bool JobInfo::DecompressToSink(zip::InputAbstract *in) {
  const zip::StreamStates ret = decomp_->DecompressStream(in, sink_);

  switch (ret) {
    case zip::kStreamEnd:
    case zip::kStreamContinue:
      return true;
    break;
    case zip::kStreamDataError:
      LogCvmfs(kLogDownload, kLogSyslogErr,
                        "(id %" PRId64 ") %s failed for input %s: bad data",
                        id_, decomp_->Describe().c_str(), url_->c_str());
      SetErrorCode(kFailBadData);
    break;
    case zip::kStreamIOError:
      LogCvmfs(kLogDownload, kLogSyslogErr,
                      "(id %" PRId64 ") %s failed for input %s: local IO error",
                      id_, decomp_->Describe().c_str(), url_->c_str());
      SetErrorCode(kFailLocalIO);
    break;
    case zip::kStreamError:
      LogCvmfs(kLogDownload, kLogSyslogErr,
                    "(id %" PRId64 ") %s failed for input %s: unhealthy status",
                    id_, decomp_->Describe().c_str(), url_->c_str());
      SetErrorCode(kFailLocalIO);
    break;
    default:
      LogCvmfs(kLogDownload, kLogSyslogErr,
                    "(id %" PRId64 ") %s failed for input %s: unknown error %d",
                    id_, decomp_->Describe().c_str(), url_->c_str(), ret);
      SetErrorCode(kFailLocalIO);
  }

  return false;
}

void JobInfo::Init() {
  id_ = atomic_xadd64(&next_uuid, 1);
  pipe_job_results = NULL;
  url_ = NULL;
  probe_hosts_ = false;
  head_request_ = false;
  follow_redirects_ = false;
  force_nocache_ = false;
  pid_ = -1;
  uid_ = -1;
  gid_ = -1;
  cred_data_ = NULL;
  interrupt_cue_ = NULL;
  sink_ = NULL;
  expected_hash_ = NULL;
  path_info_ = NULL;
  //
  range_offset_ = -1;
  range_size_ = -1;
  //
  curl_handle_ = NULL;
  headers_ = NULL;
  info_header_ = NULL;
  tracing_header_pid_ = NULL;
  tracing_header_gid_ = NULL;
  tracing_header_uid_ = NULL;
  nocache_ = false;
  error_code_ = kFailOther;
  http_code_ = -1;
  link_ = "";
  num_used_proxies_ = 0;
  num_used_metalinks_ = 0;
  num_used_hosts_ = 0;
  num_retries_ = 0;
  backoff_ms_ = 0;
  current_metalink_chain_index_ = -1;
  current_host_chain_index_ = -1;

  allow_failure_ = false;
}


/*
 * Return true if input character is escaped to 3 output characters,
 * otherwise return false and leave the input character in the first
 * output character.
 */
bool JobInfo::EscapeUrlChar(unsigned char input, char output[3]) {
  if (((input >= '0') && (input <= '9')) || ((input >= 'A') && (input <= 'Z'))
      || ((input >= 'a') && (input <= 'z')) || (input == '/') || (input == ':')
      || (input == '.') || (input == '@') || (input == '+') || (input == '-')
      || (input == '_') || (input == '~') || (input == '[') || (input == ']')
      || (input == ',')) {
    output[0] = static_cast<char>(input);
    return false;
  }

  output[0] = '%';
  output[1] = static_cast<char>((input / 16)
                                + ((input / 16 <= 9) ? '0' : 'A' - 10));
  output[2] = static_cast<char>((input % 16)
                                + ((input % 16 <= 9) ? '0' : 'A' - 10));
  return true;
}


namespace {

std::string EscapeHeader(const std::string &header) {
  std::string escaped = "";
  char escaped_char[3];
  for (std::string::const_iterator i = header.begin(); i != header.end(); i++) {
    if (JobInfo::EscapeUrlChar(*i, escaped_char)) {
      for (unsigned j = 0; j < 3; ++j) {
        escaped += escaped_char[j];
      }
    } else {
      escaped += escaped_char[0];
    }
  }

  return escaped;
}

} // namespace

/*
 * Return the filled-in template of CVMFS_INFO_HEADER
 */
std::string JobInfo::GetInfoHeaderContents(const std::string &templ) {
  enum ParseMode {
    kParseModeDefault,       // reading literal characters
    kParseModeAfterPercent,  // just saw '%'
    kParseModeInKey,         // inside '%{...}'
  };

  enum MatchMode {
    kMatchModeSkipping,    // skipping to next '\0' separator
    kMatchModeMatching,    // comparing chars against variable name
    kMatchModeCollecting,  // name matched, collecting value
  };

  std::string answer = "";
  std::string key;
  ParseMode parsemode = kParseModeDefault;
  std::vector<char> envbuf;
  bool env_attempted = false;

  for (std::string::const_iterator i = templ.begin(); i != templ.end(); i++) {
    const char c = *i;
    switch (parsemode) {
    case kParseModeDefault:
      if (c == '%') {
        parsemode = kParseModeAfterPercent;
        key = "";
      } else {
        answer += c;
      }
      break;
    case kParseModeAfterPercent:
      if (c == '{') {
        parsemode = kParseModeInKey;
      } else {
        parsemode = kParseModeDefault;
        answer += '%';
        answer += c;
      }
      break;
    case kParseModeInKey:
      if (c != '}') {
        key += c;
      } else {
        parsemode = kParseModeDefault;
        if (key == "path") {
          if (path_info_ != NULL) {
            answer += EscapeHeader(*path_info_);
          } else {
            answer += "-";
          }
        } else if (key == "pid") {
          if (pid_ != static_cast<pid_t>(-1)) {
            answer += StringifyInt(pid_);
          } else {
            answer += "-";
          }
        } else if (key == "uid") {
          if (uid_ != static_cast<uid_t>(-1)) {
            answer += StringifyInt(uid_);
          } else {
            answer += "-";
          }
        } else if (key == "gid") {
          if (gid_ != static_cast<gid_t>(-1)) {
            answer += StringifyInt(gid_);
          } else {
            answer += "-";
          }
        } else if (key.substr(0, 4) == "env:") {
          if (!env_attempted) {
            env_attempted = true;
#ifndef __APPLE__
            if (pid_ != static_cast<pid_t>(-1)) {
              ObtainDacReadSearchCapability();
              ObtainSysPtraceCapability();
              const std::string fname = "/proc/" +
                                        StringifyInt(pid_) +
                                        "/environ";
              const int fd = open(fname.c_str(), O_RDONLY);
              if (fd != -1) {
                // Unfortunately fstat does not show the size so need to
                // read it to find out the size
                ssize_t n;
                int size = 0;
                char buf[BUFSIZ];
                while ((n = read(fd, buf, BUFSIZ)) > 0) {
                  size += static_cast<int>(n);
                }
                if ((n >= 0) && (size > 0)) {
                  if (lseek(fd, 0, SEEK_SET) >= 0) {
                    envbuf.resize(size);
                    if (read(fd, envbuf.data(), size) > 0) {
                      LogCvmfs(kLogDownload, kLogDebug,
                        "(job id %" PRId64 ") read %d bytes from %s",
                        id_, size, fname.c_str());
                    } else {
                      envbuf.clear();
                    }
                  }
                }
                close(fd);
              } else {
                LogCvmfs(kLogDownload, kLogDebug,
                  "(job id %" PRId64 ") unable to open %s: %s",
                  id_, fname.c_str(), strerror(errno));
              }
              DropSysPtraceCapability();
              DropDacReadSearchCapability();
            }
#endif
          }
          if (!envbuf.empty()) {
            const char * const var = key.c_str() + 4; // everything after "env:"
            const char *varp = var;
            std::string val = "";
            MatchMode matchmode = kMatchModeMatching;
            const char * const endp = envbuf.data() + envbuf.size();
            for (const char *p = envbuf.data(); p < endp; p++) {
              switch (matchmode) {
              case kMatchModeSkipping:
                // skipping to next null character
                if (*p == '\0') {
                  varp = var;
                  matchmode = kMatchModeMatching;
                }
                break;
              case kMatchModeMatching:
                // matching variable name
                if (*p == '\0') {
                  // premature end without an '='
                  varp = var;
                } else if (*varp == *p) {
                  // so far so good
                  ++varp;
                } else if ((*varp == '\0') && (*p == '=')) {
                  // matched
                  matchmode = kMatchModeCollecting;
                } else {
                  // didn't match
                  matchmode = kMatchModeSkipping;
                }
                break;
              case kMatchModeCollecting:
                // matched, collecting value
                if (*p == '\0') {
                  // all done
                  p = endp;
                  break;
                }
                val += *p;
                break;
              }
            }
            if (val != "") {
              answer += ' ';
              answer += EscapeHeader(val);
            }
          }
        }
      }
      break;
    }
  }

  return answer;
}

}  // namespace download
