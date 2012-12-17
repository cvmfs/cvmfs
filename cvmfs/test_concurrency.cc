#include <iostream>

#include "util_concurrency.h"

class Pipeline {
   protected:
   	class Worker1 : public ConcurrentWorker<Worker1> {
	   public:
		typedef int expected_data;
		typedef int returned_data;

		struct worker_context {};

		Worker1(const worker_context *context) {}

		void operator()(const expected_data &data) {
			int val = data;
			for (int i = 0; i < data * 10000; ++i) {
				val += i;
			}

			master()->JobSuccessful(val);
		}
   	};

   	class Worker2 : public ConcurrentWorker<Worker2> {
	   public:
		typedef int  expected_data;
		typedef bool returned_data;

		struct worker_context {};

		Worker2(const worker_context *context) {}

		void operator()(const expected_data &data) {
			master()->JobSuccessful((data > 100));
		}
   	};

   	void CallbackWorker1(const Worker1::returned_data &data) {
   		workers2.Schedule(data);
   	}

   	void CallbackWorker2(const Worker2::returned_data &data) {
   		MutexLockGuard guard(mutex);
   		sum += data;
   	}

   public:
   	Pipeline() :
   		workers1(2, 100),
   		workers2(1, 100) {}

   	void Run() {
   		pthread_mutex_init(&mutex, NULL);
   		sum = 0;

		workers1.Initialize();
		workers2.Initialize();

		workers1.RegisterListener(&Pipeline::CallbackWorker1, this);
		workers2.RegisterListener(&Pipeline::CallbackWorker2, this);

		for (int i = 0; i < 1000; ++i)
			workers1.Schedule(i);

		workers1.WaitForTermination();
		workers2.WaitForEmptyQueue();
		workers2.WaitForTermination();

		pthread_mutex_destroy(&mutex);

		std::cout << sum << std::endl;
   	}

   private:
   	ConcurrentWorkers<Worker1> workers1;
   	ConcurrentWorkers<Worker2> workers2;

   	pthread_mutex_t mutex;
   	long long int sum;
};

int main() {
	Pipeline p;
	p.Run();

	return 0;
}