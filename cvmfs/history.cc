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
  insert_tag_       = new SqlInsertTag      (database_.weak_ref());
  remove_tag_       = new SqlRemoveTag      (database_.weak_ref());
  find_tag_         = new SqlFindTag        (database_.weak_ref());
  find_tag_by_date_ = new SqlFindTagByDate  (database_.weak_ref());
  count_tags_       = new SqlCountTags      (database_.weak_ref());
  list_tags_        = new SqlListTags       (database_.weak_ref());
  channel_tips_     = new SqlGetChannelTips (database_.weak_ref());
  get_hashes_       = new SqlGetHashes      (database_.weak_ref());
  return (insert_tag_ && remove_tag_ && find_tag_  && find_tag_by_date_ &&
          count_tags_ && list_tags_ && channel_tips_ && get_hashes_);
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


bool History::Exists(const std::string &name) const {
  Tag existing_tag;
  return Get(name, &existing_tag);
}


bool History::Get(const std::string &name, Tag *tag) const {
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


bool History::Get(const time_t timestamp, Tag *tag) const {
  assert (database_);
  assert (find_tag_by_date_.IsValid());
  assert (NULL != tag);

  if (! find_tag_by_date_->BindTimestamp(timestamp) ||
      ! find_tag_by_date_->FetchRow()) {
    find_tag_by_date_->Reset();
    return false;
  }

  *tag = find_tag_by_date_->RetrieveTag();
  return find_tag_by_date_->Reset();
}


bool History::List(std::vector<Tag> *tags) const {
  assert (list_tags_.IsValid());
  return RunListing(tags, list_tags_.weak_ref());
}

bool History::Tips(std::vector<Tag> *channel_tips) const {
  assert (channel_tips_.IsValid());
  return RunListing(channel_tips, channel_tips_.weak_ref());
}

template <class SqlListingT>
bool History::RunListing(std::vector<Tag> *list, SqlListingT *sql) const {
  assert (database_);
  assert (NULL != list);

  while (sql->FetchRow()) {
    list->push_back(sql->RetrieveTag());
  }

  return sql->Reset();
}


bool History::GetHashes(std::vector<shash::Any> *hashes) const {
  assert (database_);
  assert (NULL != hashes);

  while (get_hashes_->FetchRow()) {
    hashes->push_back(get_hashes_->RetrieveHash());
  }

  return get_hashes_->Reset();
}

}  // namespace history
