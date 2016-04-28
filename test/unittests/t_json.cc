/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "json_document.h"
#include "util/pointer.h"

TEST(T_Json, Empty) {
  UniquePtr<JsonDocument> json(JsonDocument::Create("{}"));
  EXPECT_TRUE(json.IsValid());
  EXPECT_EQ("{}", json->PrintCanonical());
  UniquePtr<JsonDocument> json2(JsonDocument::Create(""));
  EXPECT_FALSE(json2.IsValid());
}

TEST(T_Json, Complex) {
  UniquePtr<JsonDocument> json(JsonDocument::Create(
    "{\"string\": \"a string with spaces\",\n"
    " \"number\": 42,\n"
    " \"float\": 0.1,\n"
    " \"switch\": true,\n"
    " \"void\": null,\n"
    " \"vector\": [true, false, null, 0.0, 7, \"foo\", {1, 2}, {}, []],\n"
    " \"compound\": {\"a\": 2, \"b\": [1, 2, 3], \"c\": {}}}"));
  EXPECT_TRUE(json.IsValid());
  EXPECT_EQ(
    "{\"string\":\"a string with spaces\","
    "\"number\":42,"
    "\"float\":0.100,"
    "\"switch\":true,"
    "\"void\":null,"
    "\"vector\":[true,false,null,0.000,7,\"foo\",{1,2},{},[]],"
    "\"compound\":{\"a\":2,\"b\":[1,2,3],\"c\":{}}}",
    json->PrintCanonical());
}

TEST(T_Json, StringEscape) {
  UniquePtr<JsonDocument> json(JsonDocument::Create(
    "{\"string\": \"a \\\"string\\\" with special chars\"}"));
  ASSERT_TRUE(json.IsValid());
  EXPECT_EQ(
    "{\"string\":\"a \\\"string\\\" with special chars\"}",
    json->PrintCanonical());
}


TEST(T_Json, SearchInObject) {
  UniquePtr<JsonDocument> json(JsonDocument::Create(
    "{\"string\": \"a \\\"string\\\" with special chars\"}"));
  ASSERT_TRUE(json.IsValid());
  JSON *result = json->SearchInObject(json->root(), "string", JSON_STRING);
  EXPECT_TRUE(result != NULL);
  result = json->SearchInObject(json->root(), "string", JSON_INT);
  EXPECT_EQ(NULL, result);
  result = json->SearchInObject(json->root(), "xyz", JSON_INT);
  EXPECT_EQ(NULL, result);
  result = json->SearchInObject(NULL, "string", JSON_STRING);
  EXPECT_EQ(NULL, result);
  result =
    json->SearchInObject(json->root()->first_child, "string", JSON_STRING);
  EXPECT_EQ(NULL, result);
}
