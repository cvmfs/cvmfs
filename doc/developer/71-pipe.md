# Coding Template: `Pipe` Class

POSIX pipes are regularly used by CVMFS to communicate between different threads.

CVMFS has a templated `Pipe` class ( `cvmfs/util/pipe.h` ) that abstracts the POSIX pipes by providing different functions to read, write or close the respective pipe end (= file descriptor).
The template parameter is an enum of type `PipeType` that describes the functionality of the pipe.
This makes it clear in stack traces which pipe is blocking.

## Creating your own `Pipe` object

1. Select `PipeType` for your pipe ( also in `cvmfs/util/pipe.h` ), or add a new type if it is not available yet.
2. When using your `Pipe` object prefer handling it with a `UniquePtr`

Example:
```c++
UniquePtr<Pipe<kPipeDownloadJobsResults> > pipe_job_results;
//...
pipe_job_results = new Pipe<kPipeDownloadJobsResults>();
//... in one thread
pipe_job_results->Write<download::Failures>(error_code);
//... in another thread
pipe_job_results->Read<download::Failures>(&result);
//...
if (pipe_job_results.IsValid()) {
  pipe_job_results.Destroy();
}
```