/**
 * This file is part of the CernVM File System.
 */

#include <cstdlib>

#include "gtest/gtest.h"
#include "options.h"
#include "statistics.h"
#include "telemetry_aggregator.h"
#include "telemetry_aggregator_influx.h"
#include "util/file_guard.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace perf {

/**
 * @note
 * For all the following tests we will ignore the mountpoint as this adds
 * another layer of complexity. Here it is more important to test the general
 * function of the telemetry aggregator.
 */
class T_TelemetryAggregator : public ::testing::Test {
 protected:
  virtual void SetUp() {
    options_manager_.SetValue("CVMFS_TELEMETRY_SEND", "ON");
    options_manager_.SetValue("CVMFS_TELEMETRY_RATE", "5");
    options_manager_.SetValue("CVMFS_INFLUX_HOST", "localhost");
    options_manager_.SetValue("CVMFS_INFLUX_PORT", "8092");
    options_manager_.SetValue("CVMFS_INFLUX_METRIC_NAME", "influx_test");
    options_manager_.SetValue("CVMFS_TELEMETRY_SEND", "ON");

    c1_ = statistics_.Register("test.c1", "test counter 1");
    ASSERT_TRUE(c1_ != NULL);
    c2_ = statistics_.Register("test.c2", "test counter 2");
    ASSERT_TRUE(c2_ != NULL);
    c3_ = statistics_.Register("test.c3", "test counter 3");
    ASSERT_TRUE(c3_ != NULL);

    fqrn_ = "mytestrepo";
  }

  /**
   * Remove elements from vector that just consist of whitespace
   */
  void RemoveWhitespace(std::vector<std::string> *vec) {
    for (std::vector<std::string>::iterator itr = vec->begin();
         itr != vec->end();) {
      if (Trim(*itr).size() < 1) {
        itr = vec->erase(itr);
      } else {
        itr++;
      }
    }
  }


 protected:
  SimpleOptionsParser options_manager_;
  string fqrn_;
  perf::Statistics statistics_;
  Counter *c1_, *c2_, *c3_;
};  // class T_TelemetryAggregator

TEST_F(T_TelemetryAggregator, EmptyCounters) {
  int telemetry_send_rate_sec = 10;
  perf::TelemetryAggregatorInflux telemetry_influx(
      &statistics_, telemetry_send_rate_sec, &options_manager_, NULL, fqrn_);
  EXPECT_FALSE(telemetry_influx.is_zombie_);
  statistics_.SnapshotCounters(&telemetry_influx.counters_,
                               &telemetry_influx.timestamp_);

  string payload = telemetry_influx.MakePayload();


  string payload_header = "influx_test_absolute,repo=" + fqrn_;
  // fields are empty because counters are 0

  std::vector<std::string> payload_split = SplitString(payload, ' ');

  // remove whitespace-only strings from the vector
  RemoveWhitespace(&payload_split);

  EXPECT_EQ(payload_split.size(), 2u);
  EXPECT_STREQ(payload_header.c_str(), payload_split[0].c_str());
  EXPECT_NO_FATAL_FAILURE(String2Uint64(payload_split[1]));

  EXPECT_EQ(telemetry_influx.old_counters_.size(), 0u);
}

TEST_F(T_TelemetryAggregator, FailCreate) {
  int telemetry_send_rate_sec = 10;
  options_manager_.UnsetValue("CVMFS_INFLUX_HOST");
  perf::TelemetryAggregatorInflux telemetry_influx(
      &statistics_, telemetry_send_rate_sec, &options_manager_, NULL, fqrn_);
  EXPECT_TRUE(telemetry_influx.is_zombie_);
}

TEST_F(T_TelemetryAggregator, ExtraFields_Tags) {
  options_manager_.SetValue("CVMFS_INFLUX_EXTRA_TAGS", "test_tag=1");
  options_manager_.SetValue("CVMFS_INFLUX_EXTRA_FIELDS", "test_field=5");

  int telemetry_send_rate_sec = 10;
  perf::TelemetryAggregatorInflux telemetry_influx(
      &statistics_, telemetry_send_rate_sec, &options_manager_, NULL, fqrn_);
  EXPECT_FALSE(telemetry_influx.is_zombie_);
  statistics_.SnapshotCounters(&telemetry_influx.counters_,
                               &telemetry_influx.timestamp_);

  string payload = telemetry_influx.MakePayload();


  string payload_header = "influx_test_absolute,repo=" + fqrn_ + ",test_tag=1";
  string payload_fields = "test_field=5";
  // fields are empty because counters are 0

  std::vector<std::string> payload_split = SplitString(payload, ' ');

  // remove whitespace-only strings from the vector
  RemoveWhitespace(&payload_split);

  EXPECT_EQ(payload_split.size(), 3u);
  EXPECT_STREQ(payload_header.c_str(), payload_split[0].c_str());
  EXPECT_STREQ(payload_fields.c_str(), payload_split[1].c_str());
  EXPECT_NO_FATAL_FAILURE(String2Uint64(payload_split[2]));

  EXPECT_EQ(telemetry_influx.old_counters_.size(), 0u);
}

TEST_F(T_TelemetryAggregator, UpdateCounters_WithExtraFields_Tags) {
  options_manager_.SetValue("CVMFS_INFLUX_EXTRA_TAGS", "test_tag=1");
  options_manager_.SetValue("CVMFS_INFLUX_EXTRA_FIELDS", "test_field=5");

  int telemetry_send_rate_sec = 10;
  perf::TelemetryAggregatorInflux telemetry_influx(
      &statistics_, telemetry_send_rate_sec, &options_manager_, NULL, fqrn_);
  EXPECT_FALSE(telemetry_influx.is_zombie_);
  statistics_.SnapshotCounters(&telemetry_influx.counters_,
                               &telemetry_influx.timestamp_);

  string payload = telemetry_influx.MakePayload();


  string payload_header = "influx_test_absolute,repo=" + fqrn_ + ",test_tag=1";
  string payload_fields = "test_field=5";
  // fields are empty because counters are 0

  std::vector<std::string> payload_split = SplitString(payload, ' ');

  // remove whitespace-only strings from the vector
  RemoveWhitespace(&payload_split);

  EXPECT_EQ(payload_split.size(), 3u);
  EXPECT_STREQ(payload_header.c_str(), payload_split[0].c_str());
  EXPECT_STREQ(payload_fields.c_str(), payload_split[1].c_str());
  EXPECT_NO_FATAL_FAILURE(String2Uint64(payload_split[2]));

  EXPECT_EQ(telemetry_influx.old_counters_.size(), 0u);
  // == until here it is identical to ExtraFields_Tags


  // 2) increment counters c1 and c3
  perf::Inc(c1_);
  perf::Xadd(c3_, 10);
  telemetry_influx.counters_.swap(telemetry_influx.old_counters_);
  statistics_.SnapshotCounters(&telemetry_influx.counters_,
                               &telemetry_influx.timestamp_);

  payload_fields = "test.c1=1,test.c3=10,test_field=5";

  // ABSOLUT VALUES
  payload = telemetry_influx.MakePayload();
  payload_split = SplitString(payload, ' ');
  // remove whitespace-only strings from the vector
  RemoveWhitespace(&payload_split);

  EXPECT_EQ(payload_split.size(), 3u);
  EXPECT_STREQ(payload_header.c_str(), payload_split[0].c_str());
  EXPECT_STREQ(payload_fields.c_str(), payload_split[1].c_str());
  EXPECT_NO_FATAL_FAILURE(String2Uint64(payload_split[2]));

  // DELTA VALUES
  string delta_payload_header = "influx_test_delta,repo=" + fqrn_
                                + ",test_tag=1";
  string delta_payload_fields = "test.c1=1,test.c3=10";

  string delta_payload = telemetry_influx.MakeDeltaPayload();
  std::vector<std::string> delta_payload_split = SplitString(delta_payload,
                                                             ' ');

  EXPECT_EQ(delta_payload_split.size(), 3u);
  EXPECT_STREQ(delta_payload_header.c_str(), delta_payload_split[0].c_str());
  EXPECT_STREQ(delta_payload_fields.c_str(), delta_payload_split[1].c_str());
  EXPECT_NO_FATAL_FAILURE(String2Uint64(delta_payload_split[2]));


  // 3) increment counter c1 and c2
  perf::Inc(c2_);
  perf::Xadd(c3_, 5);
  telemetry_influx.counters_.swap(telemetry_influx.old_counters_);
  statistics_.SnapshotCounters(&telemetry_influx.counters_,
                               &telemetry_influx.timestamp_);

  payload_fields = "test.c1=1,test.c2=1,test.c3=15,test_field=5";

  // ABSOLUT VALUES
  payload = telemetry_influx.MakePayload();
  payload_split = SplitString(payload, ' ');
  // remove whitespace-only strings from the vector
  RemoveWhitespace(&payload_split);

  EXPECT_EQ(payload_split.size(), 3u);
  EXPECT_STREQ(payload_header.c_str(), payload_split[0].c_str());
  EXPECT_STREQ(payload_fields.c_str(), payload_split[1].c_str());
  EXPECT_NO_FATAL_FAILURE(String2Uint64(payload_split[2]));

  // DELTA VALUES
  delta_payload_header = "influx_test_delta,repo=" + fqrn_ + ",test_tag=1";
  delta_payload_fields = "test.c1=0,test.c2=1,test.c3=5";

  delta_payload = telemetry_influx.MakeDeltaPayload();
  delta_payload_split = SplitString(delta_payload, ' ');

  EXPECT_EQ(delta_payload_split.size(), 3u);
  EXPECT_STREQ(delta_payload_header.c_str(), delta_payload_split[0].c_str());
  EXPECT_STREQ(delta_payload_fields.c_str(), delta_payload_split[1].c_str());
  EXPECT_NO_FATAL_FAILURE(String2Uint64(delta_payload_split[2]));
}

TEST_F(T_TelemetryAggregator, SendDeltaDisabled) {
  // Test with CVMFS_INFLUX_SEND_DELTA=OFF
  options_manager_.SetValue("CVMFS_INFLUX_SEND_DELTA", "OFF");

  int telemetry_send_rate_sec = 10;
  perf::TelemetryAggregatorInflux telemetry_influx(
      &statistics_, telemetry_send_rate_sec, &options_manager_, NULL, fqrn_);
  EXPECT_FALSE(telemetry_influx.is_zombie_);

  // Increment some counters
  perf::Inc(c1_);
  perf::Xadd(c3_, 10);
  statistics_.SnapshotCounters(&telemetry_influx.counters_,
                               &telemetry_influx.timestamp_);

  string payload = telemetry_influx.MakePayload();

  // When delta sending is disabled, metric name should NOT have _absolute suffix
  string expected_header = "influx_test,repo=" + fqrn_;
  string expected_fields = "test.c1=1,test.c3=10";

  std::vector<std::string> payload_split = SplitString(payload, ' ');
  RemoveWhitespace(&payload_split);

  EXPECT_EQ(payload_split.size(), 3u);
  EXPECT_STREQ(expected_header.c_str(), payload_split[0].c_str());
  EXPECT_STREQ(expected_fields.c_str(), payload_split[1].c_str());
  EXPECT_NO_FATAL_FAILURE(String2Uint64(payload_split[2]));

  // Verify that old_counters is empty (no delta tracking)
  EXPECT_EQ(telemetry_influx.old_counters_.size(), 0u);
}

TEST_F(T_TelemetryAggregator, SendDeltaEnabledByDefault) {
  // Test that delta sending is enabled by default (no CVMFS_INFLUX_SEND_DELTA set)

  int telemetry_send_rate_sec = 10;
  perf::TelemetryAggregatorInflux telemetry_influx(
      &statistics_, telemetry_send_rate_sec, &options_manager_, NULL, fqrn_);
  EXPECT_FALSE(telemetry_influx.is_zombie_);

  // Increment some counters
  perf::Inc(c1_);
  perf::Xadd(c3_, 10);
  statistics_.SnapshotCounters(&telemetry_influx.counters_,
                               &telemetry_influx.timestamp_);

  string payload = telemetry_influx.MakePayload();

  // When delta sending is enabled (default), metric name should have _absolute suffix
  string expected_header = "influx_test_absolute,repo=" + fqrn_;
  string expected_fields = "test.c1=1,test.c3=10";

  std::vector<std::string> payload_split = SplitString(payload, ' ');
  RemoveWhitespace(&payload_split);

  EXPECT_EQ(payload_split.size(), 3u);
  EXPECT_STREQ(expected_header.c_str(), payload_split[0].c_str());
  EXPECT_STREQ(expected_fields.c_str(), payload_split[1].c_str());
  EXPECT_NO_FATAL_FAILURE(String2Uint64(payload_split[2]));
}

TEST_F(T_TelemetryAggregator, SendDeltaExplicitlyEnabled) {
  // Test with CVMFS_INFLUX_SEND_DELTA=ON
  options_manager_.SetValue("CVMFS_INFLUX_SEND_DELTA", "ON");

  int telemetry_send_rate_sec = 10;
  perf::TelemetryAggregatorInflux telemetry_influx(
      &statistics_, telemetry_send_rate_sec, &options_manager_, NULL, fqrn_);
  EXPECT_FALSE(telemetry_influx.is_zombie_);

  // Increment some counters
  perf::Inc(c1_);
  perf::Xadd(c3_, 10);
  statistics_.SnapshotCounters(&telemetry_influx.counters_,
                               &telemetry_influx.timestamp_);

  string payload = telemetry_influx.MakePayload();

  // When delta sending is explicitly enabled, metric name should have _absolute suffix
  string expected_header = "influx_test_absolute,repo=" + fqrn_;
  string expected_fields = "test.c1=1,test.c3=10";

  std::vector<std::string> payload_split = SplitString(payload, ' ');
  RemoveWhitespace(&payload_split);

  EXPECT_EQ(payload_split.size(), 3u);
  EXPECT_STREQ(expected_header.c_str(), payload_split[0].c_str());
  EXPECT_STREQ(expected_fields.c_str(), payload_split[1].c_str());
  EXPECT_NO_FATAL_FAILURE(String2Uint64(payload_split[2]));
}

}  // END namespace perf
