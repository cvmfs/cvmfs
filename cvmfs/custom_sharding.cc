// This file is part of the CernVM File System
//
#include "custom_sharding.h"

#include <assert.h>
#include <dlfcn.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "util/exception.h"
#include "util/logging.h"

#include "sharding.c"

using namespace std;  // NOLINT

CustomSharding::CustomSharding() {
  shard_ = shard_init( SHARD_CONFIG_NONE );
}

CustomSharding::~CustomSharding() {
  shard_free( shard_ );
}

void CustomSharding::StartHealthCheck() {
  shard_healthcheck_start(shard_);	
}

void CustomSharding::StopHealthCheck() {
  shard_healthcheck_stop(shard_);	
}

void CustomSharding::AddProxy(const std::string &proxy) {
  shard_add_proxy( shard_, proxy.c_str() );
}

const std::string CustomSharding::GetNextProxy(const std::string *url,
                                         const std::string &current_proxy,
                                         off_t off) {
 char **pref = shard_path( shard_, url->c_str(), off );

 std::string ret;
 int idx=0;

 if(NULL==pref) { return ret; }

 if( current_proxy != "" ) {
   shard_set_proxy_offline( shard_, current_proxy.c_str() );
 }

 ret = std::string(pref[0]);
 for(unsigned int i=0; i < shard_->N; i++ ) {
   if(!strcmp( current_proxy.c_str(), pref[i] ) ) {
     idx = (i+1) % shard_->N;
     ret =  std::string( pref[idx] );
     break;
   }
 }

 free(pref);

 return ret;
}


#ifdef __TEST__

int main(int argc, char**argv) {
	CustomSharding *c = new CustomSharding();

  for(int i=0; i<1000; i++) {
     c->AddProxy( std::string("http://proxy-anycast") );
  }

  std::string url = std::string("hello-world");
  std::string next_proxy = std::string("");
  c->StartHealthCheck();
  for(int i=0; i<10000; i++) {
     next_proxy = c->GetNextProxy( &url, next_proxy, 0ul );
  }
//  sleep(10);
  c->StopHealthCheck();
  delete c;
		
}
#endif
