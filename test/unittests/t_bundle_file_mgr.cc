/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <inttypes.h>

#include <vector>

#include "cache.h"
#include "crypto/hash.h"
#include "file_bundle.h"
#include "json_document.h"
#include "util/pointer.h"

class T_BundleFileMgr : public ::testing::Test {
 protected:
  virtual void SetUp() {
    InitObjects(42);
    UniquePtr<JsonDocument> json_doc(JsonDocument::Create(CreateJsonTxt()));
    EXPECT_TRUE(json_doc->IsValid());
    EXPECT_TRUE(json_doc.weak_ref()!=nullptr);
    bfm_.Manage(json_doc);
    EXPECT_TRUE(bfm_);
  }
  std::string ToJsonStr(const CacheManager::LabeledObject &obj) {
    constexpr size_t obj_size = 200;
    char buf[obj_size];
    std::string suf{obj.id.suffix};
    snprintf(
        buf, obj_size,
        "{\"id\":{\"algorithm\":%d,\"suffix\":\"%s\",\"digest\":\"%s\"},\"label\":{"
        "\"flags\":%d,\"size\":%" PRIu64
        ",\"zip_algorithm\":%d,\"range_offset\":%d,\"path\":\"%s\"}}",
        obj.id.algorithm, suf.c_str(), obj.id.ToString().c_str(), obj.label.flags,
        obj.label.size, obj.label.zip_algorithm,
        static_cast<int>(obj.label.range_offset), obj.label.path.c_str());
    return std::string(buf);
  }

  CacheManager::LabeledObject *ToLabeledObject(const std::string &txt_obj) {
    EXPECT_GT(txt_obj.size(), 0);
    UniquePtr<JsonDocument> jdoc(JsonDocument::Create(txt_obj));
    EXPECT_TRUE(jdoc.weak_ref() != nullptr);
    auto id_ptr = JsonDocument::SearchInObject(jdoc->root(), "id", JSON_OBJECT);
    EXPECT_TRUE(id_ptr != nullptr);
    int algorithm;
    EXPECT_TRUE(GetFromJSON<int>(id_ptr, "algorithm", &algorithm));
    std::string suffix;
    EXPECT_TRUE(GetFromJSON<std::string>(id_ptr, "suffix", &suffix));
    std::string digest;
    EXPECT_TRUE(GetFromJSON<std::string>(id_ptr, "digest", &digest));
    shash::Any id = shash::MkFromHexPtr(shash::HexPtr( digest ),suffix[0]);
    id.algorithm=static_cast<shash::Algorithms>(algorithm);
    auto label_ptr = JsonDocument::SearchInObject(jdoc->root(), "label",
                                                  JSON_OBJECT);
    EXPECT_TRUE(label_ptr != nullptr);

    int flags;
    EXPECT_TRUE(GetFromJSON<int>(label_ptr, "flags", &flags));
    int size;  ///< unzipped size, if known
    EXPECT_TRUE(GetFromJSON<int>(label_ptr, "size", &size));
    int zip_algorithm;
    EXPECT_TRUE(GetFromJSON<int>(label_ptr, "zip_algorithm", &zip_algorithm));
    int range_offset;
    EXPECT_TRUE(GetFromJSON<int>(label_ptr, "range_offset", &range_offset));
    std::string path;
    EXPECT_TRUE(GetFromJSON<std::string>(label_ptr, "path", &path));

    CacheManager::Label label{};
    label.flags = flags;
    label.size = size;
    label.zip_algorithm = static_cast<zlib::Algorithms>(zip_algorithm);
    label.range_offset = range_offset;
    label.path = path;
    return new CacheManager::LabeledObject(id, label);
  }

  virtual void TearDown() { }
  virtual ~T_BundleFileMgr() { }

  BundleFileMgr bfm_;
  std::vector<CacheManager::LabeledObject> objects_;

 private:
  void InitObjects(size_t number_of_objects) {
    for (size_t i = 0; i < number_of_objects; ++i) {
      shash::Any hash;
      hash.Randomize(i);
      CacheManager::Label label;
      label.path = "/path/to/file/" + std::to_string(i);
      objects_.push_back(CacheManager::LabeledObject(hash, label));
    }
  }
  std::string CreateJsonTxt(){
    std::string result{"{\"labeled_objects\":["};
    for(size_t i=0; i<objects_.size();++i){
      result+=ToJsonStr(objects_[i]);
    }
    result+="]}";
    return result;
  }
};

TEST_F(T_BundleFileMgr, Conversions) {
  for (size_t i = 0; i < objects_.size(); ++i) {
    auto *obj = ToLabeledObject(ToJsonStr(objects_[i]));
    EXPECT_EQ(objects_[i].id, (obj)->id);
    EXPECT_EQ(objects_[i].label, (obj)->label);
    delete obj;
  }
}

TEST_F(T_BundleFileMgr, Size) {
  EXPECT_EQ(objects_.size(),bfm_.Size());
}

