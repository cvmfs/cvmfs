#include "cvmfs_config.h"

#include <iostream>
#include <sstream>
#include <string>
#include <pthread.h>
#include <fstream>
#include <unistd.h>
#include "tracer.h"

using namespace std;

struct start_data {
   int iterations;
   int flush_every;
   int thread_id;
};

extern "C" void *thread_log (void *data) {
   int thread_id = reinterpret_cast<struct start_data *> (data)->thread_id;
   std::ostringstream o;
   o << thread_id;
   int i;
   for (i = 0; i < reinterpret_cast<struct start_data *> (data)->iterations; ++i) {
      tracer::Trace(i, o.str (), "Multi-Thread test string containing quote chars: \"");
      if ((reinterpret_cast<struct start_data *> (data)->flush_every > 0) &&
          (i % reinterpret_cast<struct start_data *> (data)->flush_every == 0))
      {
         tracer::Flush();
      }
   }
   return NULL;
}


bool check_file (string filename, int expct_nol)
{
	ifstream f;

	f.open (filename.c_str());
	int nol = 0;
	string line;
	while (!f.eof()) {
		getline (f, line);
		++nol;
	}
	f.close ();
	unlink (filename.c_str());
	return (nol == expct_nol && line == "");
}

int main () {
	cout << "Testing Tracer" << endl;

   pthread_t pthreads[10];
   struct start_data inits[10];

   cout << "Trace null-messages... " << flush;
   tracer::InitNull();
	for (int i = 0; i < 100; i++)
      tracer::Trace(i, "id", "Null");
   cout << "pass!" << endl;
   tracer::Fini();

   cout << "Create and destroy... " << flush;
   tracer::Init(5, 2, "createdestroy.trace");
	tracer::Fini();
   if (check_file("createdestroy.trace", 3))
		cout << "pass!" << endl;
	else
		cout << "FAIL!" << endl;

   cout << "Idle (5 sec)... " << flush;
   tracer::Init(10, 7, "idle.trace");
   sleep(5);
   tracer::Fini();
   cout << "pass!" << endl;

   cout << "Test single-threaded tracing 1... " << flush;
   tracer::Init(2, 0, "sthread1.trace");
   for (int i = 0; i < 100; i++)
      tracer::Trace(i, "id", "This is a test string");
   tracer::Fini();
   cout << "pass" << endl;

   cout << "Test single-threaded tracing 2... " << flush;
   tracer::Init(2, 1, "sthread2.trace");
   for (int i = 0; i < 10000; i++)
      tracer::Trace(i, "id", "This is a test string");
   tracer::Fini();
   cout << "pass" << endl;

   cout << "Test single-threaded tracing 3... " << flush;
   tracer::Init(2048, 1024, "sthread3.trace");
   for (int i = 0; i < 100000; i++)
      tracer::Trace(i, "id", "This is a test string");
   tracer::Fini();
   cout << "pass" << endl;



   cout << "Test multi-threaded tracing with 2 threads... " << flush;
   tracer::Init(2, 0, "m2thread.trace");
   for (int i = 0; i < 2; i++) {
      inits[i].iterations = 100;
      inits[i].flush_every = 0;
      inits[i].thread_id = i;
      pthread_create (&pthreads[i], NULL, thread_log, reinterpret_cast<void *>(&inits[i]));
   }
   for (int i = 0; i < 2; i++) {
      pthread_join (pthreads[i], NULL);
   }
   tracer::Fini();

   cout << "pass" << endl;
   cout << "Test multi-threaded tracing with 3 threads... " << flush;
   tracer::Init(2, 1, "m3thread.trace");
   for (int i = 0; i < 3; i++) {
      inits[i].iterations = 100;
      inits[i].flush_every = 0;
      inits[i].thread_id = i;
      pthread_create (&pthreads[i], NULL, thread_log, reinterpret_cast<void *>(&inits[i]));
   }
   for (int i = 0; i < 3; i++) {
      pthread_join (pthreads[i], NULL);
   }
   tracer::Fini();
   cout << "pass" << endl;

   cout << "Test multi-threaded tracing with 10 threads... " << flush;
   tracer::Init(8, 6, "m10thread.trace");
   for (int i = 0; i < 10; i++) {
      inits[i].iterations = 10000;
      inits[i].flush_every = 0;
      inits[i].thread_id = i;
      pthread_create (&pthreads[i], NULL, thread_log, reinterpret_cast<void *>(&inits[i]));
   }
   for (int i = 0; i < 10; i++) {
      pthread_join (pthreads[i], NULL);
   }
   tracer::Fini();
   cout << "pass" << endl;

   cout << "Test big buffer with with 10 threads... " << flush;
   tracer::Init(2048, 1024, "bigbuf.trace");
   for (int i = 0; i < 10; i++) {
      inits[i].iterations = 10000;
      inits[i].flush_every = 0;
      inits[i].thread_id = i;
      pthread_create (&pthreads[i], NULL, thread_log, reinterpret_cast<void *>(&inits[i]));
   }
   for (int i = 0; i < 10; i++) {
      pthread_join (pthreads[i], NULL);
   }
   tracer::Fini();
   cout << "pass" << endl;

   cout << "Test thread-thrashing with 2 concurrent threads... " << flush;
   tracer::Init(2, 0, "thrash2.trace");
   for (int i = 0; i < 2; i++) {
      inits[i].iterations = 100;
      inits[i].flush_every = 0;
      inits[i].thread_id = i;
   }
   for (int j = 0; j < 100; j++) {
      for (int i = 0; i < 2; i++)
         pthread_create (&pthreads[i], NULL, thread_log, reinterpret_cast<void *>(&inits[i]));
      for (int i = 0; i < 2; i++)
         pthread_join (pthreads[i], NULL);
   }
   tracer::Fini();
   cout << "pass" << endl;

   cout << "Test thread-thrashing with 3 concurrent threads... " << flush;
   tracer::Init(2, 1, "thrash3.trace");
   for (int i = 0; i < 3; i++) {
      inits[i].iterations = 100;
      inits[i].flush_every = 0;
      inits[i].thread_id = i;
   }
   for (int j = 0; j < 100; j++) {
      for (int i = 0; i < 3; i++)
         pthread_create (&pthreads[i], NULL, thread_log, reinterpret_cast<void *>(&inits[i]));
      for (int i = 0; i < 3; i++)
         pthread_join (pthreads[i], NULL);
   }
   tracer::Fini();
   cout << "pass" << endl;

   cout << "Test flushing... " << flush;
   tracer::Init(5, 2, "flush.trace");
   tracer::Flush();
   tracer::Fini();
   cout << "pass!" << endl;

   cout << "Test flushing with 2 threads... " << flush;
   tracer::Init(2, 0, "flush2.trace");
   for (int i = 0; i < 2; i++) {
      inits[i].iterations = 100;
      inits[i].flush_every = 1;
      inits[i].thread_id = i;
      pthread_create (&pthreads[i], NULL, thread_log, reinterpret_cast<void *>(&inits[i]));
   }
   for (int i = 0; i < 2; i++) {
      pthread_join (pthreads[i], NULL);
   }
   tracer::Fini();
   cout << "pass" << endl;

   cout << "Test flushing with 3 threads... " << flush;
   tracer::Init(2, 1, "flush3.trace");
   for (int i = 0; i < 3; i++) {
      inits[i].iterations = 100;
      inits[i].flush_every = 1;
      inits[i].thread_id = i;
      pthread_create (&pthreads[i], NULL, thread_log, reinterpret_cast<void *>(&inits[i]));
   }
   for (int i = 0; i < 3; i++) {
      pthread_join (pthreads[i], NULL);
   }
   tracer::Fini();
   cout << "pass" << endl;

   cout << "Test flushing with 10 threads... " << flush;
   tracer::Init(64, 32, "flush10.trace");
   for (int i = 0; i < 10; i++) {
      inits[i].iterations = 1000;
      inits[i].flush_every = 10;
      inits[i].thread_id = i;
      pthread_create (&pthreads[i], NULL, thread_log, reinterpret_cast<void *>(&inits[i]));
   }
   for (int i = 0; i < 10; i++) {
      pthread_join (pthreads[i], NULL);
   }
   tracer::Fini();
   cout << "pass" << endl;

   return 0;
}
