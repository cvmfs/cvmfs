#include "memcached.h"
#include "hash.h"
#include <string>
#include <iostream>
#include <pthread.h>

extern "C" {
   #include "debug.h"
}

using namespace std;

bool retval;
string k = "";
string data = "data";
string not_stored = "bla";
void *buf;
size_t buf_size;

void *tfunc(void *data __attribute__((unused))) {
   pmesg(D_MEMCACHED, "thread started");
   
   retval = memcached::store("127.0.0.1", 11210, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::receive("127.0.0.1", 11210, hash::t_sha1(k), &buf, &buf_size); 
   pmesg(D_MEMCACHED, "receive %s", retval ? "succeeded" : "failed");
   
   retval = memcached::store("127.0.0.1", 11211, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::receive("127.0.0.1", 11211, hash::t_sha1(k), &buf, &buf_size); 
   pmesg(D_MEMCACHED, "receive %s", retval ? "succeeded" : "failed");
   
   retval = memcached::store("127.0.0.1", 11211, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::receive("127.0.0.1", 11211, hash::t_sha1(k), &buf, &buf_size); 
   pmesg(D_MEMCACHED, "receive %s", retval ? "succeeded" : "failed");
   
   retval = memcached::remove("127.0.0.1", 11211, hash::t_sha1(k)); 
   pmesg(D_MEMCACHED, "removed %s", retval ? "succeeded" : "failed");
   
   retval = memcached::remove("127.0.0.1", 11211, hash::t_sha1(k)); 
   pmesg(D_MEMCACHED, "removed %s", retval ? "succeeded" : "failed");
   
   retval = memcached::store("127.0.0.1", 11212, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::store("127.0.0.1", 11213, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::store("127.0.0.1", 11212, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::receive("127.0.0.1", 11211, hash::t_sha1(k), &buf, &buf_size); 
   pmesg(D_MEMCACHED, "receive %s", retval ? "succeeded" : "failed");
   
   retval = memcached::store("127.0.0.1", 11211, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::receive("127.0.0.1", 11211, hash::t_sha1(not_stored), &buf, &buf_size); 
   pmesg(D_MEMCACHED, "receive %s", retval ? "succeeded" : "failed");
   
   pmesg(D_MEMCACHED, "thread ended");
   return NULL;
}

int main() {
   if (!memcached::init(2)) {
      cerr << "init failed " << endl;
      cerr << "Failed to start memcached" << endl;
      cerr << "Check your PATH variable and if you configured CernVM-FS with --enable-memcached" << endl;
      return 1;
   }
   if (!memcached::spawn()) {
      cerr << "spawn failed" << endl;
      return 1;
   }
   //memcached::fini();
   //sleep(100);
   
   retval = memcached::store("127.0.0.1", 11210, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::receive("127.0.0.1", 11210, hash::t_sha1(k), &buf, &buf_size); 
   pmesg(D_MEMCACHED, "receive %s", retval ? "succeeded" : "failed");
   
   retval = memcached::store("127.0.0.1", 11211, hash::t_sha1(data), data.c_str(), data.length()); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::receive("127.0.0.1", 11211, hash::t_sha1(data), &buf, &buf_size); 
   pmesg(D_MEMCACHED, "receive %s", retval ? "succeeded" : "failed");
   
   string cross_check((char *)buf, buf_size);
   pmesg(D_MEMCACHED, "cross check: %s (size %u)", cross_check.c_str(), buf_size);
   
   retval = memcached::store("127.0.0.1", 11211, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::receive("127.0.0.1", 11211, hash::t_sha1(k), &buf, &buf_size); 
   pmesg(D_MEMCACHED, "receive %s", retval ? "succeeded" : "failed");
   
   retval = memcached::store("127.0.0.1", 11212, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::store("127.0.0.1", 11213, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::store("127.0.0.1", 11212, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::receive("127.0.0.1", 11211, hash::t_sha1(k), &buf, &buf_size); 
   pmesg(D_MEMCACHED, "receive %s", retval ? "succeeded" : "failed");
   
   retval = memcached::store("127.0.0.1", 11211, hash::t_sha1(k), NULL, 0); 
   pmesg(D_MEMCACHED, "store %s", retval ? "succeeded" : "failed");
   
   retval = memcached::receive("127.0.0.1", 11211, hash::t_sha1(not_stored), &buf, &buf_size); 
   pmesg(D_MEMCACHED, "receive %s", retval ? "succeeded" : "failed");
   
   /* Multithread tests */
   pthread_t t[32];
   for (int i = 0; i < 32; ++i) {
      pthread_create(&t[i], NULL, tfunc, NULL);
      //pthread_join(t[i], NULL);   
   }
   for (int i = 0; i < 32; ++i)
      pthread_join(t[i], NULL);   
   
   cout << memcached::stats();
   
   memcached::fini();
   
   return 0;
}
