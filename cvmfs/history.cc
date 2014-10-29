/**
 * This file is part of the CernVM File System.
 */

#include "history.h"

#include <cstdlib>
#include <cassert>

#include <algorithm>

#include "logging.h"
#include "util.h"
#include "history_sql.h"

using namespace std;  // NOLINT

namespace history {


const std::string History::kPreviousRevisionKey = "previous_revision";

History::~History() {}


History* History::Open(const std::string &file_name) {
  const bool read_write = false;
  return Open(file_name, read_write);
}


History* History::OpenWritable(const std::string &file_name) {
  const bool read_write = true;
  return Open(file_name, read_write);
}


History* History::Open(const std::string &file_name, const bool read_write) {
  History *history = new History();
  if (NULL == history || ! history->OpenDatabase(file_name, read_write)) {
    delete history;
    return NULL;
  }

  LogCvmfs(kLogHistory, kLogDebug, "opened history database '%s' for "
                                   "repository '%s' %s",
           file_name.c_str(), history->fqrn().c_str(),
           ((history->IsWritable()) ? "(writable)" : ""));

  return history;
}


History* History::Create(const std::string &file_name,
                         const std::string &fqrn) {
  History *history = new History();
  if (NULL == history || ! history->CreateDatabase(file_name, fqrn)) {
    delete history;
    return NULL;
  }

  LogCvmfs(kLogHistory, kLogDebug, "created empty history database '%s' for"
                                   "repository '%s'",
           file_name.c_str(), fqrn.c_str());
  return history;
}


bool History::OpenDatabase(const std::string &file_name, const bool read_write)
{
  assert (! database_);
  const HistoryDatabase::OpenMode mode = (read_write)
                                           ? HistoryDatabase::kOpenReadWrite
                                           : HistoryDatabase::kOpenReadOnly;
  database_ = HistoryDatabase::Open(file_name, mode);
  if (! database_.IsValid()) {
    return false;
  }

  if (! database_->HasProperty(HistoryDatabase::kFqrnKey)) {
    LogCvmfs(kLogHistory, kLogDebug, "opened history database does not provide "
                                     "an FQRN under '%s'",
             HistoryDatabase::kFqrnKey.c_str());
    return false;
  }

  fqrn_ = database_->GetProperty<std::string>(HistoryDatabase::kFqrnKey);
  return Initialize();
}


bool History::CreateDatabase(const std::string &file_name,
                             const std::string &fqrn) {
  assert (! database_);
  assert (fqrn_.empty());
  fqrn_     = fqrn;
  database_ = HistoryDatabase::Create(file_name);
  if (! database_ || ! database_->InsertInitialValues(fqrn)) {
    LogCvmfs(kLogHistory, kLogDebug, "failed to initialize empty database '%s',"
                                     "for repository '%s'",
             file_name.c_str(), fqrn.c_str());
    return false;
  }

  return Initialize();
}


bool History::Initialize() {
  if (! PrepareQueries()) {
    LogCvmfs(kLogHistory, kLogDebug, "failed to prepare statements of history");
    return false;
  }

  return true;
}


bool History::PrepareQueries() {
  assert (database_);
  insert_tag_ = new SqlInsertTag(database_.weak_ref());
  remove_tag_ = new SqlRemoveTag(database_.weak_ref());
  find_tag_   = new SqlFindTag(database_.weak_ref());
  count_tags_ = new SqlCountTags(database_.weak_ref());
  list_tags_  = new SqlListTags(database_.weak_ref());
  return (insert_tag_ && remove_tag_ && find_tag_ && count_tags_ && list_tags_);
}


bool History::BeginTransaction()  const { return database_->BeginTransaction();  }
bool History::CommitTransaction() const { return database_->CommitTransaction(); }


bool History::SetPreviousRevision(const shash::Any &history_hash) {
  assert (database_);
  assert (IsWritable());
  return database_->SetProperty(kPreviousRevisionKey, history_hash.ToString());
}


bool History::IsWritable() const {
  assert (database_);
  return database_->read_write();
}

int History::GetNumberOfTags() const {
  assert (database_);
  assert (count_tags_.IsValid());
  bool retval = count_tags_->FetchRow();
  assert (retval);
  const int count = count_tags_->RetrieveCount();
  retval = count_tags_->Reset();
  assert (retval);
  return count;
}


bool History::Insert(const History::Tag &tag) {
  assert (database_);
  assert (insert_tag_.IsValid());

  return insert_tag_->BindTag(tag) &&
         insert_tag_->Execute()    &&
         insert_tag_->Reset();
}


bool History::Remove(const std::string &name) {
  assert (database_);
  assert (remove_tag_.IsValid());

  return remove_tag_->BindName(name) &&
         remove_tag_->Execute()      &&
         remove_tag_->Reset();
}


bool History::Find(const std::string &name, Tag *tag) const {
  assert (database_);
  assert (find_tag_.IsValid());
  assert (NULL != tag);

  if (! find_tag_->BindName(name) ||
      ! find_tag_->FetchRow()) {
    find_tag_->Reset();
    return false;
  }

  *tag = find_tag_->RetrieveTag();
  return find_tag_->Reset();
}


bool History::List(std::vector<Tag> *tags) const {
  assert (database_);
  assert (list_tags_.IsValid());
  assert (NULL != tags);

  while (list_tags_->FetchRow()) {
    tags->push_back(list_tags_->RetrieveTag());
  }

  return list_tags_->Reset();
}



//------------------------------------------------------------------------------


bool TagList::Load(HistoryDatabase *database) {
  assert(database);
  string size_field = "0";
  if (database->schema_revision() >= 1)
    size_field = "size";
  SqlTag sql_load(*database,
    // NULL for size automatically converted to 0
    "SELECT name, hash, revision, timestamp, channel, description, " +
    size_field +
    " FROM tags ORDER BY revision;");
  while (sql_load.FetchRow())
    list_.push_back(sql_load.RetrieveTag());

  return true;
}


bool TagList::Store(HistoryDatabase *database) {
  assert(database);
  SqlTag sql_erase(*database, "DELETE FROM tags;");
  bool retval = sql_erase.Execute();
  assert(retval);

  SqlTag sql_store(*database,
    "INSERT INTO tags "
    "(name, hash, revision, timestamp, channel, description, size) VALUES "
    "(:n, :h, :r, :t, :c, :d, :s);");
  for (unsigned i = 0; i < list_.size(); ++i) {
    retval = sql_store.BindTag(list_[i]);
    assert(retval);
    retval = sql_store.Execute();
    if (!retval) {
      LogCvmfs(kLogHistory, kLogStderr, "failed to store taglist (%s)",
               database->GetLastErrorMsg().c_str());
      abort();
    }
    sql_store.Reset();
  }

  return true;
}


string TagList::List() {
  string result =
    "NAME | HASH | SIZE | REVISION | TIMESTAMP | CHANNEL | DESCRIPTION\n";
  for (unsigned i = 0; i < list_.size(); ++i) {
    Tag tag(list_[i]);
    string tag_size = "n/a";
    if (tag.size > 0 && tag.size < 1024)
      tag_size = StringifyInt(tag.size);
    else if (tag.size >= 1024 && tag.size < 1024*1024)
      tag_size = StringifyInt(tag.size/1024) + "kB";
    else if (tag.size >= 1024*1024)
      tag_size = StringifyInt(tag.size/(1024*1024)) + "MB";
    result += tag.name + " | " + tag.root_hash.ToString() + " | " +
              tag_size + " | " +
              StringifyInt(tag.revision) + " | " +
              StringifyTime(tag.timestamp, true) + " | " +
              StringifyInt(tag.channel) + " | " + tag.description + "\n";
  }
  return result;
}


bool TagList::FindTag(const string &name, Tag *tag) {
  assert(tag);
  for (unsigned i = 0; i < list_.size(); ++i) {
    if (list_[i].name == name) {
      *tag = list_[i];
      return true;
    }
  }
  return false;
}


bool TagList::FindTagByDate(const time_t seconds_utc, Tag *tag) {
  assert(tag);
  bool result = false;
  for (unsigned i = 0; i < list_.size(); ++i) {
    if (list_[i].timestamp > seconds_utc)
      break;
    *tag = list_[i];
    result = true;
  }
  return result;
}


bool TagList::FindRevision(const unsigned revision, Tag *tag) {
  assert(tag);
  for (unsigned i = 0; i < list_.size(); ++i) {
    if (list_[i].revision == revision) {
      *tag = list_[i];
      return true;
    }
  }
  return false;
}


bool TagList::FindHash(const shash::Any &hash, Tag *tag) {
  assert(tag);
  for (unsigned i = 0; i < list_.size(); ++i) {
    if (list_[i].root_hash == hash) {
      *tag = list_[i];
      return true;
    }
  }
  return false;
}


void TagList::Remove(const string &name) {
  for (vector<Tag>::iterator i = list_.begin(); i < list_.end(); ++i) {
    if (i->name == name) {
      list_.erase(i);
      return;
    }
  }
}


TagList::Failures TagList::Insert(const Tag &tag) {
  Tag existing_tag;
  if (FindTag(tag.name, &existing_tag))
    return kFailTagExists;

  list_.push_back(tag);
  return kFailOk;
}


map<string, shash::Any> TagList::GetAllHashes() {
  map<string, shash::Any> result;
  for (unsigned i = 0; i < list_.size(); ++i) {
    result[list_[i].name] = list_[i].root_hash;
  }
  return result;
}


struct revision_comparator {
  bool operator() (const Tag &tag1, const Tag &tag2) {
    return tag1.revision < tag2.revision;
  }
};

struct hash_extractor {
  const shash::Any& operator() (const Tag &tag) {
    return tag.root_hash;
  }
};

std::vector<shash::Any> TagList::GetReferencedHashes() const {
  // copy the internal tag list and sort it by decending revision
  std::vector<Tag> tags = list_;
  std::sort(tags.begin(), tags.end(), revision_comparator());

  // extract the root catalog hashes from it
  std::vector<shash::Any> hashes(tags.size());
  std::transform(tags.begin(), tags.end(), hashes.begin(), hash_extractor());
  return hashes;
}


// Ordered list, newest releases first
vector<TagList::ChannelTag> TagList::GetChannelTops() {
  vector<ChannelTag> result;
  if (list_.size() == 0)
    return result;

  vector<Tag> sorted_tag_list(list_);
  sort(sorted_tag_list.begin(), sorted_tag_list.end());

  set<UpdateChannel> processed_channels;
  for (int i = sorted_tag_list.size()-1; i >= 0; --i) {
    UpdateChannel channel = sorted_tag_list[i].channel;
    if (channel == kChannelTrunk)
      continue;
    if (processed_channels.find(channel) == processed_channels.end()) {
      result.push_back(ChannelTag(channel, sorted_tag_list[i].root_hash));
      processed_channels.insert(channel);
    }
  }
  return result;
}


void TagList::Rollback(const unsigned until_revision) {
  for (vector<Tag>::iterator i = list_.begin(); i < list_.end(); ) {
    if (i->revision >= until_revision)
      i = list_.erase(i);
    else
      ++i;
  }
}

}  // namespace history
