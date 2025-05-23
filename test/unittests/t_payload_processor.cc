/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "pack.h"
#include "receiver/payload_processor.h"
#include "util/concurrency.h"
#include "util/string.h"

using namespace receiver;  // NOLINT

class MockPayloadProcessor : public PayloadProcessor {
 public:
  MockPayloadProcessor() : PayloadProcessor(), num_files_received_(0) { }
  virtual ~MockPayloadProcessor() { }

  virtual Result Initialize() { return kSuccess; }

  virtual Result Finalize() { return kSuccess; }

  virtual void Upload(const std::string &source, const std::string &dest) { }

  virtual void ConsumerEventCallback(const ObjectPackBuild::Event & /*event*/) {
    num_files_received_++;
  }

  int num_files_received_;
};

class T_PayloadProcessor : public ::testing::Test {
 protected:
  T_PayloadProcessor()
      : ready_(1, 1)
      , thread_()
      , read_fd_()
      , write_fd_()
      , serializer_(NULL)
      , digest_(shash::kSha1) { }

  virtual void SetUp() {
    int fds[2];
    ASSERT_NE(-1, pipe(fds));
    read_fd_ = fds[0];

    // Prepare an object pack and send it through the pipe
    ObjectPack::BucketHandle hd = pack_.NewBucket();

    std::vector<uint8_t> buffer(4096, 0);
    ObjectPack::AddToBucket(&buffer[0], 4096, hd);
    shash::Any buffer_hash(shash::kSha1);
    shash::HashMem(&buffer[0], buffer.size(), &buffer_hash);
    ASSERT_TRUE(pack_.CommitBucket(ObjectPack::kCas, buffer_hash, hd));

    serializer_ = new ObjectPackProducer(&pack_);

    ASSERT_TRUE(serializer_);

    serializer_->GetDigest(&digest_);

    write_fd_ = fds[1];

    ASSERT_EQ(0, pthread_create(&thread_, NULL, T_PayloadProcessor::Worker,
                                static_cast<void *>(this)));

    ASSERT_EQ(true, ready_.Dequeue());
  }

  virtual void TearDown() {
    ASSERT_EQ(0, pthread_join(thread_, NULL));

    close(read_fd_);

    delete serializer_;
  }

  static void *Worker(void *data) {
    T_PayloadProcessor *ctx = static_cast<T_PayloadProcessor *>(data);

    ctx->ready_.Enqueue(true);

    int nb_written = 0;
    int nb_produced = 0;
    do {
      std::vector<unsigned char> buffer(4096, 0);
      nb_produced = ctx->serializer_->ProduceNext(buffer.size(), &buffer[0]);
      nb_written = write(ctx->write_fd_, &buffer[0], nb_produced);
    } while (nb_produced > 0 && nb_written > 0);

    close(ctx->write_fd_);

    return NULL;
  }

  FifoChannel<bool> ready_;
  pthread_t thread_;
  int read_fd_;
  int write_fd_;
  ObjectPack pack_;
  ObjectPackProducer *serializer_;
  shash::Any digest_;
};

TEST_F(T_PayloadProcessor, Basic) {
  MockPayloadProcessor proc;
  ASSERT_EQ(0, proc.num_files_received_);
  ASSERT_EQ(PayloadProcessor::kSuccess,
            proc.Process(read_fd_, digest_.ToString(false), "some_path",
                         serializer_->GetHeaderSize()));
  ASSERT_EQ(1, proc.num_files_received_);
}
