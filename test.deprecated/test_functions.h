#ifndef TEST_FUNCTIONS_CC
#define TEST_FUNCTIONS_CC 1

#include <time.h>
#include <stdlib.h>

class StopWatch {
private:
   struct timespec start_ticks_;
   struct timespec stop_ticks_;
   bool running_;
   
public:
   StopWatch() {
      running_ = false;
   }
   
   inline void start() {
      if (isRunning()) {
         return;
      }
      
      running_ = true;
   	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start_ticks_);
   }
   
   inline void stop() {
      if (not isRunning()) {
         return;
      }
      
   	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &stop_ticks_);
      running_ = false;
   }
   
   inline void reset() {} // unused
   
   // returns the time in seconds
   inline double getTime() const {
      double start_seconds = start_ticks_.tv_sec + ((double)start_ticks_.tv_nsec / 1000000000.0);
      double end_seconds = stop_ticks_.tv_sec + ((double)stop_ticks_.tv_nsec / 1000000000.0);
      return end_seconds - start_seconds;
   }
   
   inline bool isRunning() const { return running_; }
};
inline int getRandomValueBetween(const int a, const int b) {
   return rand() % (b - a + 1) + a;
}

#endif /* TEST_FUNCTIONS_CC */
