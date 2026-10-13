/**
 * This file is part of the CernVM File System.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "nfs4/filehandle.h"
#include "nfs4/nfs4_const.h"
#include "nfs4/nfs4_server.h"
#include "nfs4/rpc.h"
#include "nfs4/xdr.h"

using namespace nfs4;  // NOLINT

namespace {

class MemoryBackend : public Backend {
 public:
  struct Node {
    Node() : parent(0) { }
    Attr attr;
    uint64_t parent;
    std::map<std::string, uint64_t> children;
    std::string data;
    std::map<std::string, std::string> xattrs;
  };

  MemoryBackend()
      : generation_(1), num_forgets_(0), read_delay_ms_(0), next_ino_(100) {
    Node &root = nodes_[kRoot];
    root.attr.ino = kRoot;
    root.attr.mode = S_IFDIR | 0755;
    root.attr.nlink = 2;
    root.parent = kRoot;
  }

  uint64_t AddDir(uint64_t parent, const std::string &name) {
    return Add(parent, name, S_IFDIR | 0755, "");
  }
  uint64_t AddFile(uint64_t parent, const std::string &name,
                   const std::string &data, mode_t mode = 0644) {
    return Add(parent, name, S_IFREG | mode, data);
  }
  uint64_t AddLink(uint64_t parent, const std::string &name,
                   const std::string &target) {
    return Add(parent, name, S_IFLNK | 0777, target);
  }
  Node *node(uint64_t ino) { return &nodes_[ino]; }

  virtual uint64_t RootInode() { return kRoot; }
  virtual uint64_t FsId() { return 42; }
  virtual uint64_t Generation() { return generation_; }

  virtual int GetAttr(uint64_t ino, Attr *attr) {
    if (nodes_.count(ino) == 0)
      return ENOENT;
    *attr = nodes_[ino].attr;
    return 0;
  }
  virtual int Lookup(uint64_t parent, const std::string &name, Attr *attr) {
    std::map<std::string, uint64_t>::const_iterator
        it = nodes_[parent].children.find(name);
    if (it == nodes_[parent].children.end())
      return ENOENT;
    *attr = nodes_[it->second].attr;
    return 0;
  }
  virtual int LookupParent(uint64_t ino, Attr *attr) {
    *attr = nodes_[nodes_[ino].parent].attr;
    return 0;
  }
  virtual int ReadLink(uint64_t ino, std::string *target) {
    *target = nodes_[ino].data;
    return 0;
  }
  virtual int ReadDir(uint64_t ino, std::vector<DirEntry> *entries) {
    entries->clear();
    for (std::map<std::string, uint64_t>::const_iterator it =
             nodes_[ino].children.begin();
         it != nodes_[ino].children.end();
         ++it) {
      entries->push_back(DirEntry(it->first, it->second));
    }
    return 0;
  }
  virtual int Read(uint64_t ino, uint64_t offset, uint32_t size,
                   std::string *data) {
    if (read_delay_ms_ > 0)
      usleep(read_delay_ms_ * 1000);
    const std::string &content = nodes_[ino].data;
    data->clear();
    if (offset < content.size())
      *data = content.substr(offset, size);
    return 0;
  }
  virtual void Forget(uint64_t /* ino */) { num_forgets_++; }
  virtual int StatFs(FsStat *stat) {
    stat->bytes_total = 1000;
    return 0;
  }
  virtual int ListXattr(uint64_t ino, std::vector<std::string> *names) {
    names->clear();
    for (std::map<std::string, std::string>::const_iterator it =
             nodes_[ino].xattrs.begin();
         it != nodes_[ino].xattrs.end();
         ++it) {
      names->push_back(it->first);
    }
    return 0;
  }
  virtual int GetXattr(uint64_t ino, const std::string &name,
                       const Credentials & /* creds */, std::string *value) {
    std::map<std::string, std::string>::const_iterator
        it = nodes_[ino].xattrs.find(name);
    if (it == nodes_[ino].xattrs.end())
      return ENODATA;
    *value = it->second;
    return 0;
  }

  static const uint64_t kRoot = 1;
  uint64_t generation_;
  unsigned num_forgets_;
  unsigned read_delay_ms_;

 private:
  uint64_t Add(uint64_t parent, const std::string &name, mode_t mode,
               const std::string &data) {
    const uint64_t ino = next_ino_++;
    Node &n = nodes_[ino];
    n.attr.ino = ino;
    n.attr.mode = mode;
    n.attr.size = data.size();
    n.attr.uid = 1000;
    n.attr.gid = 1000;
    n.attr.mtime.sec = 1700000000;
    n.attr.ctime.sec = 1700000000;
    n.parent = parent;
    n.data = data;
    nodes_[parent].children[name] = ino;
    return ino;
  }

  std::map<uint64_t, Node> nodes_;
  uint64_t next_ino_;
};
const uint64_t MemoryBackend::kRoot;


/** Builds the argument array of a COMPOUND call */
class Compound {
 public:
  explicit Compound(uint32_t minor_version = 0) : num_ops_(0) {
    args_.PutString("test");
    args_.PutU32(minor_version);
    count_offset_ = args_.ReserveU32();
  }

  XdrWriter *Op(uint32_t op) {
    args_.PutU32(op);
    args_.PatchU32(count_offset_, ++num_ops_);
    return &args_;
  }
  void PutRootFh() { Op(OP_PUTROOTFH); }
  void PutFh(const std::string &fh) { Op(OP_PUTFH)->PutOpaque(fh); }
  void Lookup(const std::string &name) { Op(OP_LOOKUP)->PutString(name); }
  void GetFh() { Op(OP_GETFH); }
  void GetAttr(uint32_t word0, uint32_t word1 = 0) {
    XdrWriter *w = Op(OP_GETATTR);
    w->PutU32(2);
    w->PutU32(word0);
    w->PutU32(word1);
  }
  void Read(uint32_t seqid, const std::string &other, uint64_t offset,
            uint32_t count) {
    XdrWriter *w = Op(OP_READ);
    w->PutU32(seqid);
    w->PutFixedOpaque(other.data(), kStateidOtherSize);
    w->PutU64(offset);
    w->PutU32(count);
  }
  void ReadAnonymous(uint64_t offset, uint32_t count) {
    Read(0, std::string(kStateidOtherSize, '\0'), offset, count);
  }
  void Readdir(uint64_t cookie, const std::string &verifier, uint32_t maxcount,
               uint32_t word0 = 0) {
    XdrWriter *w = Op(OP_READDIR);
    w->PutU64(cookie);
    w->PutFixedOpaque(verifier.data(), kNfs4VerifierSize);
    w->PutU32(maxcount);
    w->PutU32(maxcount);
    w->PutU32(1);
    w->PutU32(word0);
  }

  const std::string &buffer() const { return args_.buffer(); }

 private:
  XdrWriter args_;
  size_t count_offset_;
  uint32_t num_ops_;
};


/** Parsed COMPOUND reply; op results are left in the reader */
struct Reply {
  Reply() : status(0), num_results(0), reader(NULL, 0) { }
  uint32_t status;
  uint32_t num_results;
  std::string raw;
  XdrReader reader;

  /** Reads the next op header and returns its status */
  uint32_t NextOp(uint32_t expected_op) {
    uint32_t op, op_status;
    EXPECT_TRUE(reader.GetU32(&op));
    EXPECT_EQ(expected_op, op);
    EXPECT_TRUE(reader.GetU32(&op_status));
    return op_status;
  }
};


class T_Nfs4Server : public ::testing::Test {
 protected:
  virtual void SetUp() {
    dir_ = backend_.AddDir(MemoryBackend::kRoot, "dir");
    file_ = backend_.AddFile(dir_, "file", "hello world");
    link_ = backend_.AddLink(dir_, "link", "file");
    secret_ = backend_.AddFile(dir_, "secret", "s3cr3t", 0600);
    backend_.node(file_)->xattrs["user.revision"] = "17";
    backend_.node(file_)->xattrs["user.hash"] = "abc";
    server_ = new Nfs4Server(&backend_, Nfs4Server::Options());
    call_.procedure = kNfsProcCompound;
    call_.cred_flavor = kAuthSys;
    call_.creds.uid = 1000;
    call_.creds.gid = 1000;
  }
  virtual void TearDown() { delete server_; }

  void Run(const Compound &compound, Reply *reply) {
    XdrReader args(
        reinterpret_cast<const unsigned char *>(compound.buffer().data()),
        compound.buffer().size());
    XdrWriter res;
    EXPECT_EQ(kAcceptSuccess, server_->Dispatch(call_, &args, &res));
    reply->raw = res.buffer();
    reply->reader = XdrReader(
        reinterpret_cast<const unsigned char *>(reply->raw.data()),
        reply->raw.size());
    std::string tag;
    reply->reader.GetU32(&reply->status);
    reply->reader.GetString(&tag);
    EXPECT_EQ("test", tag);
    reply->reader.GetU32(&reply->num_results);
  }

  std::string FhOf(const std::string &path) {
    Compound c;
    c.PutRootFh();
    size_t start = 0;
    while (start < path.size()) {
      size_t end = path.find('/', start);
      if (end == std::string::npos)
        end = path.size();
      c.Lookup(path.substr(start, end - start));
      start = end + 1;
    }
    c.GetFh();
    Reply r;
    Run(c, &r);
    EXPECT_EQ(NFS4_OK, r.status);
    for (uint32_t i = 0; i + 1 < r.num_results; ++i) {
      uint32_t op, status;
      r.reader.GetU32(&op);
      r.reader.GetU32(&status);
    }
    std::string fh;
    EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.NextOp(OP_GETFH));
    r.reader.GetOpaque(&fh);
    return fh;
  }

  /** Returns the client id after SETCLIENTID + SETCLIENTID_CONFIRM */
  uint64_t EstablishClient(const std::string &name) {
    Compound c;
    XdrWriter *w = c.Op(OP_SETCLIENTID);
    w->PutFixedOpaque("verifier", kNfs4VerifierSize);
    w->PutOpaque(name);
    w->PutU32(0x40000000);
    w->PutString("tcp");
    w->PutString("127.0.0.1.4.1");
    w->PutU32(1);
    Reply r;
    Run(c, &r);
    EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.NextOp(OP_SETCLIENTID));
    uint64_t clientid;
    unsigned char confirm[kNfs4VerifierSize];
    r.reader.GetU64(&clientid);
    r.reader.GetFixedOpaque(confirm, kNfs4VerifierSize);

    Compound c2;
    w = c2.Op(OP_SETCLIENTID_CONFIRM);
    w->PutU64(clientid);
    w->PutFixedOpaque(confirm, kNfs4VerifierSize);
    Reply r2;
    Run(c2, &r2);
    EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r2.status);
    return clientid;
  }

  /** OPEN of dir/<name>; returns the op status and the stateid */
  uint32_t Open(uint64_t clientid, const std::string &owner, uint32_t seqid,
                const std::string &name, uint32_t access,
                uint32_t *stateid_seqid, std::string *other,
                uint32_t *rflags = NULL) {
    Compound c;
    c.PutFh(FhOf("dir"));
    XdrWriter *w = c.Op(OP_OPEN);
    w->PutU32(seqid);
    w->PutU32(access);
    w->PutU32(OPEN4_SHARE_DENY_NONE);
    w->PutU64(clientid);
    w->PutOpaque(owner);
    w->PutU32(OPEN4_NOCREATE);
    w->PutU32(CLAIM_NULL);
    w->PutString(name);
    Reply r;
    Run(c, &r);
    EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.NextOp(OP_PUTFH));
    const uint32_t status = r.NextOp(OP_OPEN);
    if (status == NFS4_OK) {
      unsigned char buf[kStateidOtherSize];
      r.reader.GetU32(stateid_seqid);
      r.reader.GetFixedOpaque(buf, kStateidOtherSize);
      other->assign(reinterpret_cast<char *>(buf), kStateidOtherSize);
      bool atomic;
      uint64_t before, after;
      uint32_t flags;
      r.reader.GetBool(&atomic);
      r.reader.GetU64(&before);
      r.reader.GetU64(&after);
      r.reader.GetU32(&flags);
      if (rflags)
        *rflags = flags;
    }
    return status;
  }

  uint32_t Close(uint32_t seqid, uint32_t stateid_seqid,
                 const std::string &other, uint32_t *new_seqid = NULL) {
    Compound c;
    c.PutFh(FhOf("dir/file"));
    XdrWriter *w = c.Op(OP_CLOSE);
    w->PutU32(seqid);
    w->PutU32(stateid_seqid);
    w->PutFixedOpaque(other.data(), kStateidOtherSize);
    Reply r;
    Run(c, &r);
    r.NextOp(OP_PUTFH);
    const uint32_t status = r.NextOp(OP_CLOSE);
    if (status == NFS4_OK && new_seqid)
      r.reader.GetU32(new_seqid);
    return status;
  }

  MemoryBackend backend_;
  Nfs4Server *server_;
  RpcCall call_;
  uint64_t dir_, file_, link_, secret_;
};

}  // anonymous namespace


TEST(T_Nfs4Xdr, RoundTrip) {
  XdrWriter w;
  w.PutU32(0xdeadbeef);
  w.PutU64(0x0102030405060708ull);
  w.PutBool(true);
  w.PutString("abcde");
  w.PutFixedOpaque("xy", 2);
  EXPECT_EQ(4u + 8 + 4 + (4 + 8) + 4, w.size());

  XdrReader r(reinterpret_cast<const unsigned char *>(w.buffer().data()),
              w.size());
  uint32_t u32;
  uint64_t u64;
  bool b;
  std::string s;
  unsigned char fixed[2];
  EXPECT_TRUE(r.GetU32(&u32));
  EXPECT_EQ(0xdeadbeef, u32);
  EXPECT_TRUE(r.GetU64(&u64));
  EXPECT_EQ(0x0102030405060708ull, u64);
  EXPECT_TRUE(r.GetBool(&b));
  EXPECT_TRUE(b);
  EXPECT_TRUE(r.GetString(&s));
  EXPECT_EQ("abcde", s);
  EXPECT_TRUE(r.GetFixedOpaque(fixed, 2));
  EXPECT_EQ(0, memcmp(fixed, "xy", 2));
  EXPECT_EQ(0u, r.remaining());
  EXPECT_FALSE(r.GetU32(&u32));
  EXPECT_FALSE(r.ok());
}


TEST(T_Nfs4Xdr, Limits) {
  XdrWriter w;
  w.PutString("toolong");
  XdrReader r(reinterpret_cast<const unsigned char *>(w.buffer().data()),
              w.size());
  std::string s;
  EXPECT_FALSE(r.GetString(&s, 3));
  EXPECT_FALSE(r.ok());

  // Length field larger than the buffer
  XdrWriter w2;
  w2.PutU32(1000);
  XdrReader r2(reinterpret_cast<const unsigned char *>(w2.buffer().data()),
               w2.size());
  EXPECT_FALSE(r2.GetOpaque(&s));

  XdrWriter w3;
  w3.PutU32(2);
  XdrReader r3(reinterpret_cast<const unsigned char *>(w3.buffer().data()),
               w3.size());
  bool b;
  EXPECT_FALSE(r3.GetBool(&b));
}


TEST(T_Nfs4FileHandle, EncodeDecode) {
  const FileHandle fh(FileHandle::kNamedAttr, 7, 123456789, HashName("x"));
  const std::string raw = fh.Encode();
  EXPECT_EQ(static_cast<size_t>(FileHandle::kSize), raw.size());
  FileHandle decoded;
  EXPECT_TRUE(FileHandle::Decode(raw, &decoded));
  EXPECT_TRUE(fh == decoded);

  for (unsigned i = 0; i < raw.size(); ++i) {
    std::string corrupt = raw;
    corrupt[i] ^= 0x01;
    EXPECT_FALSE(FileHandle::Decode(corrupt, &decoded)) << "byte " << i;
  }
  EXPECT_FALSE(FileHandle::Decode(raw.substr(1), &decoded));
  EXPECT_FALSE(FileHandle::Decode("", &decoded));
}


TEST_F(T_Nfs4Server, RpcEnvelope) {
  XdrWriter w;
  w.PutU32(77);  // xid
  w.PutU32(kRpcCall);
  w.PutU32(kRpcVersion);
  w.PutU32(kNfsProgram);
  w.PutU32(3);  // NFSv3: not served
  w.PutU32(kNfsProcNull);
  w.PutU32(kAuthNone);
  w.PutU32(0);
  w.PutU32(kAuthNone);
  w.PutU32(0);
  std::string reply;
  ASSERT_TRUE(HandleRpcRecord(server_, w.buffer(), &reply));
  XdrReader r(reinterpret_cast<const unsigned char *>(reply.data()),
              reply.size());
  uint32_t xid, type, reply_stat, verf_flavor, verf_len, accept, low, high;
  r.GetU32(&xid);
  r.GetU32(&type);
  r.GetU32(&reply_stat);
  r.GetU32(&verf_flavor);
  r.GetU32(&verf_len);
  r.GetU32(&accept);
  r.GetU32(&low);
  r.GetU32(&high);
  EXPECT_TRUE(r.ok());
  EXPECT_EQ(77u, xid);
  EXPECT_EQ(kRpcReply, type);
  EXPECT_EQ(kMsgAccepted, reply_stat);
  EXPECT_EQ(static_cast<uint32_t>(kAcceptProgMismatch), accept);
  EXPECT_EQ(4u, low);
  EXPECT_EQ(4u, high);

  // Truncated call: answered with GARBAGE_ARGS
  ASSERT_TRUE(HandleRpcRecord(server_, w.buffer().substr(0, 20), &reply));
  // Not even an xid
  EXPECT_FALSE(HandleRpcRecord(server_, "ab", &reply));
}


TEST_F(T_Nfs4Server, LookupGetattrRead) {
  Compound c;
  c.PutRootFh();
  c.Lookup("dir");
  c.Lookup("file");
  c.GetAttr((1u << FATTR4_TYPE) | (1u << FATTR4_SIZE) | (1u << FATTR4_FILEID));
  c.ReadAnonymous(6, 100);
  Reply r;
  Run(c, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.status);
  EXPECT_EQ(5u, r.num_results);
  r.NextOp(OP_PUTROOTFH);
  r.NextOp(OP_LOOKUP);
  r.NextOp(OP_LOOKUP);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.NextOp(OP_GETATTR));
  uint32_t bitmap_len, word0, word1, type;
  std::string values;
  r.reader.GetU32(&bitmap_len);
  EXPECT_EQ(1u, bitmap_len);
  r.reader.GetU32(&word0);
  EXPECT_EQ((1u << FATTR4_TYPE) | (1u << FATTR4_SIZE) | (1u << FATTR4_FILEID),
            word0);
  r.reader.GetOpaque(&values);
  XdrReader v(reinterpret_cast<const unsigned char *>(values.data()),
              values.size());
  uint64_t size, fileid;
  v.GetU32(&type);
  v.GetU64(&size);
  v.GetU64(&fileid);
  EXPECT_EQ(static_cast<uint32_t>(NF4REG), type);
  EXPECT_EQ(11u, size);
  EXPECT_EQ(file_, fileid);
  (void)word1;

  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.NextOp(OP_READ));
  bool eof;
  std::string data;
  r.reader.GetBool(&eof);
  r.reader.GetOpaque(&data);
  EXPECT_TRUE(eof);
  EXPECT_EQ("world", data);
}


TEST_F(T_Nfs4Server, ErrorsStopCompound) {
  Compound c;
  c.PutRootFh();
  c.Lookup("nonexistent");
  c.GetFh();
  Reply r;
  Run(c, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_NOENT), r.status);
  EXPECT_EQ(2u, r.num_results);

  Compound c2;
  c2.GetFh();
  Run(c2, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_NOFILEHANDLE), r.status);

  Compound c3;
  c3.PutRootFh();
  c3.Lookup("dir");
  c3.Lookup("file");
  c3.Lookup("x");
  Run(c3, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_NOTDIR), r.status);

  Compound c4;
  c4.PutRootFh();
  c4.Lookup("..");
  Run(c4, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_BADNAME), r.status);

  Compound c5;
  c5.PutFh("garbage");
  Run(c5, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_BADHANDLE), r.status);

  Compound c6;
  c6.PutFh(FileHandle(FileHandle::kObject, 43, file_).Encode());
  Run(c6, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_STALE), r.status);

  Compound c7;
  c7.PutFh(FileHandle(FileHandle::kObject, 42, 99999).Encode());
  c7.GetAttr(1u << FATTR4_TYPE);
  Run(c7, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_STALE), r.status);

  Compound c8;
  c8.Op(12345);
  Run(c8, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_OP_ILLEGAL), r.status);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_OP_ILLEGAL), r.NextOp(OP_ILLEGAL));

  Compound c9(1);
  c9.PutRootFh();
  Run(c9, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_MINOR_VERS_MISMATCH), r.status);
  EXPECT_EQ(0u, r.num_results);
}


TEST_F(T_Nfs4Server, ReadOnly) {
  Compound c;
  c.PutRootFh();
  c.Op(OP_REMOVE)->PutString("dir");
  Reply r;
  Run(c, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_ROFS), r.status);

  Compound c2;
  c2.PutFh(FhOf("dir/file"));
  XdrWriter *w = c2.Op(OP_SETATTR);
  w->PutU32(0);
  w->PutFixedOpaque(std::string(kStateidOtherSize, '\0').data(),
                    kStateidOtherSize);
  w->PutU32(0);
  w->PutU32(0);
  Run(c2, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_ROFS), r.status);
  r.NextOp(OP_PUTFH);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_ROFS), r.NextOp(OP_SETATTR));
  uint32_t attrsset_len;
  EXPECT_TRUE(r.reader.GetU32(&attrsset_len));
  EXPECT_EQ(0u, attrsset_len);
  EXPECT_EQ(0u, r.reader.remaining());
}


TEST_F(T_Nfs4Server, Access) {
  Compound c;
  c.PutFh(FhOf("dir/secret"));
  c.Op(OP_ACCESS)->PutU32(ACCESS4_READ | ACCESS4_MODIFY);
  Reply r;
  Run(c, &r);
  r.NextOp(OP_PUTFH);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.NextOp(OP_ACCESS));
  uint32_t supported, granted;
  r.reader.GetU32(&supported);
  r.reader.GetU32(&granted);
  EXPECT_EQ(ACCESS4_READ | ACCESS4_MODIFY, supported);
  EXPECT_EQ(ACCESS4_READ, granted);

  call_.creds.uid = 2000;
  call_.creds.gid = 2000;
  Run(c, &r);
  r.NextOp(OP_PUTFH);
  r.NextOp(OP_ACCESS);
  r.reader.GetU32(&supported);
  r.reader.GetU32(&granted);
  EXPECT_EQ(0u, granted);
}


TEST_F(T_Nfs4Server, Readlink) {
  Compound c;
  c.PutFh(FhOf("dir/link"));
  c.Op(OP_READLINK);
  Reply r;
  Run(c, &r);
  r.NextOp(OP_PUTFH);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.NextOp(OP_READLINK));
  std::string target;
  r.reader.GetString(&target);
  EXPECT_EQ("file", target);
}


TEST_F(T_Nfs4Server, Lookupp) {
  Compound c;
  c.PutFh(FhOf("dir/file"));
  c.Op(OP_LOOKUPP);
  Reply r;
  Run(c, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_NOTDIR), r.status);

  Compound c2;
  c2.PutFh(FhOf("dir"));
  c2.Op(OP_LOOKUPP);
  c2.GetFh();
  Run(c2, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.status);
  r.NextOp(OP_PUTFH);
  r.NextOp(OP_LOOKUPP);
  r.NextOp(OP_GETFH);
  std::string fh;
  r.reader.GetOpaque(&fh);
  EXPECT_EQ(FileHandle(FileHandle::kObject, 42, MemoryBackend::kRoot).Encode(),
            fh);

  Compound c3;
  c3.PutRootFh();
  c3.Op(OP_LOOKUPP);
  Run(c3, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_NOENT), r.status);
}


TEST_F(T_Nfs4Server, ReaddirPaging) {
  const unsigned kNumFiles = 50;
  const uint64_t big = backend_.AddDir(MemoryBackend::kRoot, "big");
  for (unsigned i = 0; i < kNumFiles; ++i) {
    char name[32];
    snprintf(name, sizeof(name), "file%03u", i);
    backend_.AddFile(big, name, "x");
  }
  const std::string fh = FhOf("big");

  std::vector<std::string> names;
  uint64_t cookie = 0;
  std::string verifier(kNfs4VerifierSize, '\0');
  bool eof = false;
  unsigned rounds = 0;
  while (!eof) {
    ASSERT_LT(rounds++, kNumFiles);
    Compound c;
    c.PutFh(fh);
    c.Readdir(cookie, verifier, 300, 1u << FATTR4_TYPE);
    Reply r;
    Run(c, &r);
    r.NextOp(OP_PUTFH);
    ASSERT_EQ(static_cast<uint32_t>(NFS4_OK), r.NextOp(OP_READDIR));
    unsigned char verf[kNfs4VerifierSize];
    r.reader.GetFixedOpaque(verf, kNfs4VerifierSize);
    verifier.assign(reinterpret_cast<char *>(verf), kNfs4VerifierSize);
    bool follows;
    unsigned in_this_round = 0;
    while (r.reader.GetBool(&follows) && follows) {
      std::string name, attrs;
      uint32_t bitmap_len, word;
      r.reader.GetU64(&cookie);
      r.reader.GetString(&name);
      r.reader.GetU32(&bitmap_len);
      for (uint32_t i = 0; i < bitmap_len; ++i)
        r.reader.GetU32(&word);
      r.reader.GetOpaque(&attrs);
      names.push_back(name);
      in_this_round++;
    }
    r.reader.GetBool(&eof);
    ASSERT_TRUE(r.reader.ok());
    ASSERT_GT(in_this_round, 0u);
  }
  EXPECT_GT(rounds, 1u);
  ASSERT_EQ(kNumFiles, names.size());
  EXPECT_EQ("file000", names[0]);
  EXPECT_EQ("file049", names[kNumFiles - 1]);

  // A changed generation invalidates outstanding cookies
  backend_.generation_++;
  Compound c;
  c.PutFh(fh);
  c.Readdir(5, verifier, 300);
  Reply r;
  Run(c, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_NOT_SAME), r.status);

  Compound c2;
  c2.PutFh(fh);
  c2.Readdir(0, verifier, 20);
  Run(c2, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_TOOSMALL), r.status);
}


TEST_F(T_Nfs4Server, OpenReadClose) {
  const uint64_t clientid = EstablishClient("client-a");
  uint32_t seqid, flags;
  std::string other;
  ASSERT_EQ(static_cast<uint32_t>(NFS4_OK),
            Open(clientid, "owner", 1, "file", OPEN4_SHARE_ACCESS_READ, &seqid,
                 &other, &flags));
  EXPECT_EQ(1u, seqid);
  EXPECT_EQ(0u, flags & OPEN4_RESULT_CONFIRM);
  EXPECT_EQ(1u, server_->NumOpenStates());

  // Opening the same file again by the same owner bumps the stateid
  uint32_t seqid2;
  std::string other2;
  ASSERT_EQ(static_cast<uint32_t>(NFS4_OK),
            Open(clientid, "owner", 2, "file", OPEN4_SHARE_ACCESS_READ, &seqid2,
                 &other2));
  EXPECT_EQ(other, other2);
  EXPECT_EQ(2u, seqid2);
  EXPECT_EQ(1u, server_->NumOpenStates());

  Compound c;
  c.PutFh(FhOf("dir/file"));
  c.Read(seqid2, other2, 0, 5);
  Reply r;
  Run(c, &r);
  r.NextOp(OP_PUTFH);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.NextOp(OP_READ));
  bool eof;
  std::string data;
  r.reader.GetBool(&eof);
  r.reader.GetOpaque(&data);
  EXPECT_FALSE(eof);
  EXPECT_EQ("hello", data);

  // Stateid of a different file
  Compound c2;
  c2.PutFh(FhOf("dir/secret"));
  c2.Read(seqid2, other2, 0, 5);
  Run(c2, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_BAD_STATEID), r.status);

  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_OLD_STATEID), Close(3, seqid, other));
  uint32_t closed_seqid;
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK),
            Close(3, seqid2, other2, &closed_seqid));
  EXPECT_EQ(3u, closed_seqid);
  EXPECT_EQ(0u, server_->NumOpenStates());
  EXPECT_EQ(1u, backend_.num_forgets_);

  // Retransmitted CLOSE is answered from the replay cache
  closed_seqid = 0;
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK),
            Close(3, seqid2, other2, &closed_seqid));
  EXPECT_EQ(3u, closed_seqid);

  // Out-of-order seqid
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_BAD_SEQID),
            Open(clientid, "owner", 9, "file", OPEN4_SHARE_ACCESS_READ, &seqid,
                 &other));
}


TEST_F(T_Nfs4Server, OpenErrors) {
  const uint64_t clientid = EstablishClient("client-b");
  uint32_t seqid;
  std::string other;
  EXPECT_EQ(
      static_cast<uint32_t>(NFS4ERR_ROFS),
      Open(clientid, "o", 1, "file", OPEN4_SHARE_ACCESS_BOTH, &seqid, &other));
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_NOENT),
            Open(clientid, "o", 2, "missing", OPEN4_SHARE_ACCESS_READ, &seqid,
                 &other));
  EXPECT_EQ(
      static_cast<uint32_t>(NFS4ERR_SYMLINK),
      Open(clientid, "o", 3, "link", OPEN4_SHARE_ACCESS_READ, &seqid, &other));
  call_.creds.uid = 2000;
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_ACCESS),
            Open(clientid, "o", 4, "secret", OPEN4_SHARE_ACCESS_READ, &seqid,
                 &other));
  // Client id of a previous server instance
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_STALE_CLIENTID),
            Open(clientid ^ (1ull << 40), "o", 5, "file",
                 OPEN4_SHARE_ACCESS_READ, &seqid, &other));
  EXPECT_EQ(0u, server_->NumOpenStates());
}


TEST_F(T_Nfs4Server, ClientReboot) {
  const uint64_t first = EstablishClient("client-c");
  uint32_t seqid;
  std::string other;
  ASSERT_EQ(
      static_cast<uint32_t>(NFS4_OK),
      Open(first, "owner", 1, "file", OPEN4_SHARE_ACCESS_READ, &seqid, &other));

  Compound renew;
  renew.Op(OP_RENEW)->PutU64(first);
  Reply r;
  Run(renew, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.status);

  // Same client name with a new verifier: the old incarnation and its opens
  // go away on confirmation
  Compound c;
  XdrWriter *w = c.Op(OP_SETCLIENTID);
  w->PutFixedOpaque("verif-2", kNfs4VerifierSize);
  w->PutOpaque(std::string("client-c"));
  w->PutU32(0);
  w->PutString("tcp");
  w->PutString("127.0.0.1.4.1");
  w->PutU32(1);
  Run(c, &r);
  r.NextOp(OP_SETCLIENTID);
  uint64_t second;
  unsigned char confirm[kNfs4VerifierSize];
  r.reader.GetU64(&second);
  r.reader.GetFixedOpaque(confirm, kNfs4VerifierSize);
  EXPECT_NE(first, second);
  EXPECT_EQ(1u, server_->NumOpenStates());

  Compound c2;
  w = c2.Op(OP_SETCLIENTID_CONFIRM);
  w->PutU64(second);
  w->PutFixedOpaque(confirm, kNfs4VerifierSize);
  Run(c2, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.status);
  EXPECT_EQ(0u, server_->NumOpenStates());

  Run(renew, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_STALE_CLIENTID), r.status);
}


TEST_F(T_Nfs4Server, NamedAttributes) {
  Compound c;
  c.PutFh(FhOf("dir/file"));
  c.Op(OP_OPENATTR)->PutBool(false);
  c.Readdir(0, std::string(kNfs4VerifierSize, '\0'), 4096);
  Reply r;
  Run(c, &r);
  r.NextOp(OP_PUTFH);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.NextOp(OP_OPENATTR));
  ASSERT_EQ(static_cast<uint32_t>(NFS4_OK), r.NextOp(OP_READDIR));
  unsigned char verf[kNfs4VerifierSize];
  r.reader.GetFixedOpaque(verf, kNfs4VerifierSize);
  std::vector<std::string> names;
  bool follows;
  while (r.reader.GetBool(&follows) && follows) {
    uint64_t cookie;
    std::string name, attrs;
    uint32_t bitmap_len, word;
    r.reader.GetU64(&cookie);
    r.reader.GetString(&name);
    r.reader.GetU32(&bitmap_len);
    for (uint32_t i = 0; i < bitmap_len; ++i)
      r.reader.GetU32(&word);
    r.reader.GetOpaque(&attrs);
    names.push_back(name);
  }
  ASSERT_EQ(2u, names.size());
  EXPECT_EQ("user.hash", names[0]);
  EXPECT_EQ("user.revision", names[1]);

  Compound c2;
  c2.PutFh(FhOf("dir/file"));
  c2.Op(OP_OPENATTR)->PutBool(false);
  c2.Lookup("user.revision");
  c2.GetAttr((1u << FATTR4_TYPE) | (1u << FATTR4_SIZE));
  c2.ReadAnonymous(0, 100);
  c2.Op(OP_LOOKUPP);
  c2.Op(OP_LOOKUPP);
  c2.GetFh();
  Run(c2, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.status);
  r.NextOp(OP_PUTFH);
  r.NextOp(OP_OPENATTR);
  r.NextOp(OP_LOOKUP);
  r.NextOp(OP_GETATTR);
  uint32_t bitmap_len, word0, type;
  uint64_t size;
  std::string values;
  r.reader.GetU32(&bitmap_len);
  r.reader.GetU32(&word0);
  r.reader.GetOpaque(&values);
  XdrReader v(reinterpret_cast<const unsigned char *>(values.data()),
              values.size());
  v.GetU32(&type);
  v.GetU64(&size);
  EXPECT_EQ(static_cast<uint32_t>(NF4NAMEDATTR), type);
  EXPECT_EQ(2u, size);
  r.NextOp(OP_READ);
  bool eof;
  std::string data;
  r.reader.GetBool(&eof);
  r.reader.GetOpaque(&data);
  EXPECT_TRUE(eof);
  EXPECT_EQ("17", data);
  r.NextOp(OP_LOOKUPP);
  r.NextOp(OP_LOOKUPP);
  r.NextOp(OP_GETFH);
  std::string fh;
  r.reader.GetOpaque(&fh);
  EXPECT_EQ(FhOf("dir/file"), fh);

  Compound c3;
  c3.PutFh(FhOf("dir/file"));
  c3.Op(OP_OPENATTR)->PutBool(false);
  c3.Lookup("user.missing");
  Run(c3, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_NOENT), r.status);
}


TEST_F(T_Nfs4Server, Verify) {
  // Ask for the size, then VERIFY/NVERIFY with that exact value
  Compound c;
  c.PutFh(FhOf("dir/file"));
  c.GetAttr(1u << FATTR4_SIZE);
  Reply r;
  Run(c, &r);
  r.NextOp(OP_PUTFH);
  r.NextOp(OP_GETATTR);
  uint32_t bitmap_len, word0;
  std::string values;
  r.reader.GetU32(&bitmap_len);
  r.reader.GetU32(&word0);
  r.reader.GetOpaque(&values);

  Compound c2;
  c2.PutFh(FhOf("dir/file"));
  XdrWriter *w = c2.Op(OP_VERIFY);
  w->PutU32(1);
  w->PutU32(1u << FATTR4_SIZE);
  w->PutOpaque(values);
  Run(c2, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), r.status);

  Compound c3;
  c3.PutFh(FhOf("dir/file"));
  w = c3.Op(OP_NVERIFY);
  w->PutU32(1);
  w->PutU32(1u << FATTR4_SIZE);
  w->PutOpaque(values);
  Run(c3, &r);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_SAME), r.status);
}


TEST_F(T_Nfs4Server, TcpServer) {
  RpcServer::Options options;
  options.workers = 2;
  RpcServer rpc(server_, options);
  ASSERT_TRUE(rpc.Listen("127.0.0.1", 0));
  ASSERT_NE(0, rpc.port());
  rpc.Start();

  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(rpc.port());
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(
      0, connect(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)));

  // A COMPOUND split over two record fragments
  Compound compound;
  compound.PutRootFh();
  compound.GetFh();
  XdrWriter call;
  call.PutU32(1234);
  call.PutU32(kRpcCall);
  call.PutU32(kRpcVersion);
  call.PutU32(kNfsProgram);
  call.PutU32(kNfsVersion4);
  call.PutU32(kNfsProcCompound);
  call.PutU32(kAuthNone);
  call.PutU32(0);
  call.PutU32(kAuthNone);
  call.PutU32(0);
  call.PutRaw(compound.buffer().data(), compound.buffer().size());
  const std::string &msg = call.buffer();
  const size_t split = 10;
  uint32_t marker = htonl(split);
  ASSERT_EQ(4, write(fd, &marker, 4));
  ASSERT_EQ(static_cast<ssize_t>(split), write(fd, msg.data(), split));
  marker = htonl(0x80000000u | (msg.size() - split));
  ASSERT_EQ(4, write(fd, &marker, 4));
  ASSERT_EQ(static_cast<ssize_t>(msg.size() - split),
            write(fd, msg.data() + split, msg.size() - split));

  uint32_t reply_marker;
  ASSERT_EQ(4, read(fd, &reply_marker, 4));
  reply_marker = ntohl(reply_marker);
  EXPECT_TRUE(reply_marker & 0x80000000u);
  std::string reply(reply_marker & 0x7fffffffu, '\0');
  size_t got = 0;
  while (got < reply.size()) {
    const ssize_t n = read(fd, &reply[got], reply.size() - got);
    ASSERT_GT(n, 0);
    got += n;
  }
  XdrReader r(reinterpret_cast<const unsigned char *>(reply.data()),
              reply.size());
  uint32_t xid, type, reply_stat, verf_flavor, verf_len, accept, status;
  r.GetU32(&xid);
  r.GetU32(&type);
  r.GetU32(&reply_stat);
  r.GetU32(&verf_flavor);
  r.GetU32(&verf_len);
  r.GetU32(&accept);
  r.GetU32(&status);
  EXPECT_EQ(1234u, xid);
  EXPECT_EQ(static_cast<uint32_t>(kAcceptSuccess), accept);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), status);

  close(fd);
  rpc.Stop();
}


TEST_F(T_Nfs4Server, DelaySlowCompound) {
  Nfs4Server::Options options;
  options.delay_after_ms = 50;
  options.executor_threads = 2;
  Nfs4Server server(&backend_, options);
  backend_.read_delay_ms_ = 300;

  Compound c;
  c.PutFh(FhOf("dir/file"));
  c.ReadAnonymous(0, 5);
  XdrReader args(reinterpret_cast<const unsigned char *>(c.buffer().data()),
                 c.buffer().size());
  XdrWriter res;
  EXPECT_EQ(kAcceptSuccess, server.Dispatch(call_, &args, &res));
  XdrReader r(reinterpret_cast<const unsigned char *>(res.buffer().data()),
              res.size());
  uint32_t status, num_results, op, op_status;
  std::string tag;
  r.GetU32(&status);
  r.GetString(&tag);
  r.GetU32(&num_results);
  r.GetU32(&op);
  r.GetU32(&op_status);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_DELAY), status);
  EXPECT_EQ("test", tag);
  EXPECT_EQ(1u, num_results);
  EXPECT_EQ(static_cast<uint32_t>(OP_PUTFH), op);
  EXPECT_EQ(static_cast<uint32_t>(NFS4ERR_DELAY), op_status);

  // Fast requests complete within the deadline
  backend_.read_delay_ms_ = 0;
  XdrReader args2(reinterpret_cast<const unsigned char *>(c.buffer().data()),
                  c.buffer().size());
  XdrWriter res2;
  server.Dispatch(call_, &args2, &res2);
  XdrReader r2(reinterpret_cast<const unsigned char *>(res2.buffer().data()),
               res2.size());
  r2.GetU32(&status);
  EXPECT_EQ(static_cast<uint32_t>(NFS4_OK), status);
}
