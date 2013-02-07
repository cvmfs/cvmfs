#include <iostream>
#include <algorithm>

#include "util.h"
#include "upload.h"

using namespace upload;

void SplCallback(const upload::SpoolerResult &result) {
  std::cout << "spooler sent callback" << std::endl;

  if (result.IsChunked()) {
    std::cout << "CHUNKS:" << std::endl;
  }

  FileChunks::const_iterator i    = result.file_chunks.begin();
  FileChunks::const_iterator iend = result.file_chunks.end();
  for (; i != iend; ++i) {
    std::cout << "offset: " << i->offset() << " size: " << i->size() << " hash: " << i->content_hash().ToString() << std::endl;
  }

  std::cout << "Bulk: " << result.content_hash.ToString() << std::endl;
}

int main() {

  std::string sds = "riak:/home/rene/Documents/Schweinestall/cvmfs/build/ramdisk:http://cernvmbl005:8098/riak/cvmfs@http://cernvmbl006:8098/riak/cvmfs@http://cernvmbl007:8098/riak/cvmfs@http://cernvmbl008:8098/riak/cvmfs@http://cernvmbl009:8098/riak/cvmfs";
  SpoolerDefinition sd(sds, true, 4000000, 8000000, 16000000);

  UniquePtr<AbstractSpooler> spooler(AbstractSpooler::Construct(sd));
  spooler->RegisterListener(&SplCallback);

  spooler->Process("/mnt/atlas-condb/cond12_data.000002.lar.COND._0003.pool.root");
  spooler->Process("/mnt/atlas-condb/cond12_data.000002.lar.COND._0002.pool.root");
  spooler->Process("/mnt/atlas-condb/cond12_data.000002.lar.COND._0001.pool.root");
  spooler->Process("/mnt/atlas-condb/cond12_data.000002.lar.COND._0004.pool.root");
  spooler->Process("/mnt/atlas-condb/cond12_data.000002.lar.COND._0005.pool.root");

  spooler->WaitForTermination();

  return 0;
}

