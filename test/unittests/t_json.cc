/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <utility>

#include "json_document.h"
#include "json_document_write.h"
#include "util/pointer.h"

TEST(T_Json, Empty) {
  std::unique_ptr<JsonDocument> json(JsonDocument::Create("{}"));
  EXPECT_TRUE(json.IsValid());
  EXPECT_EQ("{}", json->PrintCanonical());
  std::unique_ptr<JsonDocument> json2(JsonDocument::Create(""));
  EXPECT_FALSE(json2.IsValid());
}

TEST(T_Json, Complex) {
  std::unique_ptr<JsonDocument> json(JsonDocument::Create(
      "{\"string\": \"a string with spaces\",\n"
      " \"number\": 42,\n"
      " \"float\": 0.1,\n"
      " \"switch\": true,\n"
      " \"void\": null,\n"
      " \"vector\": [true, false, null, 0.0, 7, \"foo\", [1, 2], {}, []],\n"
      " \"compound\": {\"a\": 2, \"b\": [1, 2, 3], \"c\": {}}}"));
  EXPECT_TRUE(json.IsValid());
  EXPECT_EQ("{\"compound\":{\"a\":2,\"b\":[1,2,3],\"c\":{}},"
            "\"float\":0.1,"
            "\"number\":42,"
            "\"string\":\"a string with spaces\","
            "\"switch\":true,"
            "\"vector\":[true,false,null,0.0,7,\"foo\",[1,2],{},[]],"
            "\"void\":null}",
            json->PrintCanonical());
}

TEST(T_Json, StringEscape) {
  std::unique_ptr<JsonDocument> json(JsonDocument::Create(
      "{\"string\": \"a \\\"string\\\" with special chars\"}"));
  ASSERT_TRUE(json.IsValid());
  EXPECT_EQ("{\"string\":\"a \\\"string\\\" with special chars\"}",
            json->PrintCanonical());
}

TEST(T_Json, SearchInObject) {
  std::unique_ptr<JsonDocument> json(JsonDocument::Create(
      "{\"string\": \"a \\\"string\\\" with special chars\"}"));
  ASSERT_TRUE(json.IsValid());
  const JSON *result = json->SearchInObject(
      json->root(), "string", JSON_STRING);
  EXPECT_TRUE(result != NULL);
  result = json->SearchInObject(json->root(), "string", JSON_INT);
  EXPECT_EQ(NULL, result);
  result = json->SearchInObject(json->root(), "xyz", JSON_INT);
  EXPECT_EQ(NULL, result);
  result = json->SearchInObject(NULL, "string", JSON_STRING);
  EXPECT_EQ(NULL, result);
}

TEST(T_Json, GenerateValidJsonString) {
  JsonStringGenerator input;
  input.Add("f1", "v1");
  input.Add("f2", "v2");
  input.Add("f3", "v3");
  input.Add("f4", "v\n4");
  input.Add("integer", 12);

  std::string output = input.GenerateString();

  ASSERT_EQ("{\"f1\":\"v1\",\"f2\":\"v2\",\"f3\":\"v3\","
            "\"f4\":\"v\\n4\",\"integer\":12}",
            output);

  std::unique_ptr<JsonDocument> json(JsonDocument::Create(output));
  ASSERT_TRUE(json.IsValid());
}
