/**
 * This file is part of the CernVM File System.
 */

#include <string>

#include "cvmfs_config.h"

#include <cassert>

#include "logging.h"
#include "swissknife.h"

#include "swissknife_check.h"
#include "swissknife_diff.h"
#include "swissknife_gc.h"
#include "swissknife_graft.h"
#include "swissknife_hash.h"
#include "swissknife_history.h"
#include "swissknife_info.h"
#include "swissknife_ingest.h"
#include "swissknife_lease.h"
#include "swissknife_letter.h"
#include "swissknife_lsrepo.h"
#include "swissknife_migrate.h"
#include "swissknife_pull.h"
#include "swissknife_reflog.h"
#include "swissknife_scrub.h"
#include "swissknife_sign.h"
#include "swissknife_sync.h"
#include "swissknife_zpipe.h"
#include "util/string.h"

using namespace std;  // NOLINT

struct RevisionFlags {
  enum T {
    kInitialRevision   = 1,
    kUpdatableRevision = 2,
    kUpdatedRevision   = 3,
    kFailingRevision   = 4,
  };
};

class StatisticsDatabase : public sqlite::Database<StatisticsDatabase> {
 public:
  // not const - needs to be adaptable!
  static float        kLatestSchema;
  // not const - needs to be adaptable!
  static unsigned     kLatestSchemaRevision;
  static const float  kLatestCompatibleSchema;
  static bool         compacting_fails;
  static unsigned int  instances;
  unsigned int         create_empty_db_calls;
  unsigned int         check_compatibility_calls;
  unsigned int         live_upgrade_calls;
  mutable unsigned int compact_calls;

  bool CreateEmptyDatabase() {
    ++create_empty_db_calls;

  return sqlite::Sql(sqlite_db(),
    "CREATE TABLE foobar (foo TEXT, bar TEXT, "
    "  CONSTRAINT pk_foo PRIMARY KEY (foo))").Execute();
  }

  bool CheckSchemaCompatibility() {
    ++check_compatibility_calls;
    return (schema_version() > kLatestCompatibleSchema - 0.1 &&
            schema_version() < kLatestCompatibleSchema + 0.1);
  }

  bool LiveSchemaUpgradeIfNecessary() {
    ++live_upgrade_calls;
    const unsigned int revision = schema_revision();

    if (revision == RevisionFlags::kInitialRevision) {
      return true;
    }

    if (revision == RevisionFlags::kUpdatableRevision) {
      set_schema_revision(RevisionFlags::kUpdatedRevision);
      StoreSchemaRevision();
      return true;
    }

    if (revision == RevisionFlags::kFailingRevision) {
      return false;
    }

    return false;
  }

  bool CompactDatabase() const {
    ++compact_calls;
    return !compacting_fails;
  }

  ~StatisticsDatabase() {
    --StatisticsDatabase::instances;
  }

 protected:
  // TODO(rmeusel): C++11 - constructor inheritance
  friend class sqlite::Database<StatisticsDatabase>;
  StatisticsDatabase(const std::string  &filename,
                const OpenMode      open_mode) :
    sqlite::Database<StatisticsDatabase>(filename, open_mode),
    create_empty_db_calls(0),  check_compatibility_calls(0),
    live_upgrade_calls(0), compact_calls(0)
  {
    ++StatisticsDatabase::instances;
  }
};

const float    StatisticsDatabase::kLatestCompatibleSchema = 1.0f;
float          StatisticsDatabase::kLatestSchema           = 1.0f;
unsigned       StatisticsDatabase::kLatestSchemaRevision   = RevisionFlags::kInitialRevision;
unsigned int   StatisticsDatabase::instances               = 0;
bool           StatisticsDatabase::compacting_fails        = false;

typedef vector<swissknife::Command *> Commands;
Commands command_list;

void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "CernVM-FS repository storage management commands\n"
    "Version %s\n"
    "Usage (normally called from cvmfs_server):\n"
    "  cvmfs_swissknife <command> [options]\n",
    VERSION);

  for (unsigned i = 0; i < command_list.size(); ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "\n"
             "Command %s\n"
             "--------", command_list[i]->GetName().c_str());
    for (unsigned j = 0; j < command_list[i]->GetName().length(); ++j) {
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "-");
    }
    LogCvmfs(kLogCvmfs, kLogStdout, "");
    LogCvmfs(kLogCvmfs, kLogStdout, "%s",
             command_list[i]->GetDescription().c_str());
    swissknife::ParameterList params = command_list[i]->GetParams();
    if (!params.empty()) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Options:");
      for (unsigned j = 0; j < params.size(); ++j) {
        LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "  -%c    %s",
                 params[j].key(), params[j].description().c_str());
        if (params[j].optional())
          LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, " (optional)");
        LogCvmfs(kLogCvmfs, kLogStdout, "");
      }
    }  // Parameter list
  }  // Command list

  LogCvmfs(kLogCvmfs, kLogStdout, "");
}


int main(int argc, char **argv) {
  command_list.push_back(new swissknife::CommandCreate());
  command_list.push_back(new swissknife::CommandUpload());
  command_list.push_back(new swissknife::CommandRemove());
  command_list.push_back(new swissknife::CommandPeek());
  command_list.push_back(new swissknife::CommandSync());
  command_list.push_back(new swissknife::CommandApplyDirtab());
  command_list.push_back(new swissknife::CommandEditTag());
  command_list.push_back(new swissknife::CommandListTags());
  command_list.push_back(new swissknife::CommandInfoTag());
  command_list.push_back(new swissknife::CommandRollbackTag());
  command_list.push_back(new swissknife::CommandEmptyRecycleBin());
  command_list.push_back(new swissknife::CommandSign());
  command_list.push_back(new swissknife::CommandLetter());
  command_list.push_back(new swissknife::CommandCheck());
  command_list.push_back(new swissknife::CommandListCatalogs());
  command_list.push_back(new swissknife::CommandDiff());
  command_list.push_back(new swissknife::CommandPull());
  command_list.push_back(new swissknife::CommandZpipe());
  command_list.push_back(new swissknife::CommandGraft());
  command_list.push_back(new swissknife::CommandHash());
  command_list.push_back(new swissknife::CommandInfo());
  command_list.push_back(new swissknife::CommandVersion());
  command_list.push_back(new swissknife::CommandMigrate());
  command_list.push_back(new swissknife::CommandScrub());
  command_list.push_back(new swissknife::CommandGc());
  command_list.push_back(new swissknife::CommandReconstructReflog());
  command_list.push_back(new swissknife::CommandLease());
  command_list.push_back(new swissknife::Ingest());

  if (argc < 2) {
    Usage();
    return 1;
  }
  if ((string(argv[1]) == "--help")) {
    Usage();
    return 0;
  }
  if ((string(argv[1]) == "--version")) {
    swissknife::CommandVersion().Main(swissknife::ArgumentList());
    return 0;
  }

  // find the command to be run
  swissknife::Command *command = NULL;
  for (unsigned i = 0; i < command_list.size(); ++i) {
    if (command_list[i]->GetName() == string(argv[1])) {
      command = command_list[i];
      break;
    }
  }

  if (NULL == command) {
    Usage();
    return 1;
  }

  bool display_statistics = false;

  // parse the command line arguments for the Command
  swissknife::ArgumentList args;
  optind = 1;
  string option_string = "";
  swissknife::ParameterList params = command->GetParams();
  for (unsigned j = 0; j < params.size(); ++j) {
    option_string.push_back(params[j].key());
    if (!params[j].switch_only())
      option_string.push_back(':');
  }
  // Now adding the generic -+ extra option command
  option_string.push_back(swissknife::Command::kGenericParam);
  option_string.push_back(':');
  int c;
  while ((c = getopt(argc, argv, option_string.c_str())) != -1) {
    bool valid_option = false;
    for (unsigned j = 0; j < params.size(); ++j) {
      if (c == params[j].key()) {
        assert(c != swissknife::Command::kGenericParam);
        valid_option = true;
        string *argument = NULL;
        if (!params[j].switch_only()) {
          argument = new string(optarg);
        }
        args[c] = argument;
        break;
      }
    }
    if (c == swissknife::Command::kGenericParam) {
      valid_option = true;
      vector<string> flags = SplitString(optarg,
                                         swissknife::
                                         Command::kGenericParamSeparator);
      for (unsigned i = 0; i < flags.size(); ++i) {
        if (flags[i] == "stats") {
          display_statistics = true;
        }
      }
    }
    if (!valid_option) {
      Usage();
      return 1;
    }
  }
  for (unsigned j = 0; j < params.size(); ++j) {
    if (!params[j].optional()) {
      if (args.find(params[j].key()) == args.end()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "parameter -%c missing",
                 params[j].key());
        return 1;
      }
    }
  }

  // run the command
  const int retval = command->Main(args);
  if (display_statistics) {
    // get the repo name
    string repo_name = *args.find('N')->second;
    // LogCvmfs(kLogCvmfs, kLogStdout, "For repository [%s]", repo_name.c_str());
    // create a new database file if is not already there
    StatisticsDatabase *db = StatisticsDatabase::Create(repo_name + ".db");

    LogCvmfs(kLogCvmfs, kLogStdout, "Command statistics");
    LogCvmfs(kLogCvmfs, kLogStdout, "%s",
             command->statistics()
             ->PrintList(perf::Statistics::kPrintHeader).c_str());

    delete db;
  }

  // delete the command list
        Commands::const_iterator i    = command_list.begin();
  const Commands::const_iterator iend = command_list.end();
  for (; i != iend; ++i) {
    delete *i;
  }
  command_list.clear();

  return retval;
}
