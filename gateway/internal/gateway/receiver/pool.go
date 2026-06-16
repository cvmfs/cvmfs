package receiver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	stats "github.com/cvmfs/gateway/internal/gateway/statistics"
)

// task is the common interface of all receiver tasks
type task interface {
	Reply() chan<- error
	Context() context.Context
}

// payloadTask is the input data for a payload submission task
type payloadTask struct {
	ctx        context.Context
	leasePath  string
	payload    io.Reader
	digest     string
	headerSize int
	replyChan  chan<- error
}

// Reply returns the reply channel
func (p payloadTask) Reply() chan<- error {
	return p.replyChan
}

// Context returns the context associated with the task
func (p payloadTask) Context() context.Context {
	return p.ctx
}

// commitTask is the input data for a commit task
type commitTask struct {
	ctx          context.Context
	leasePath    string
	oldRootHash  string
	newRootHash  string
	tag          gw.RepositoryTag
	directGraft  bool
	replyChan    chan<- error
	finalRevChan chan<- uint64
}

// Reply returns the reply channel
func (p commitTask) Reply() chan<- error {
	return p.replyChan
}

// Context returns the context associated with the task
func (p commitTask) Context() context.Context {
	return p.ctx
}

type testCrashTask struct {
	ctx       context.Context
	replyChan chan<- error
}

func (p testCrashTask) Reply() chan<- error {
	return p.replyChan
}

func (p testCrashTask) Context() context.Context {
	return p.ctx
}

// Pool maintains a number of parallel receiver workers to service payload
// submission and commit requests. Each worker owns a persistent
// cvmfs_receiver process that is reused across tasks; a new process is
// spawned only when the previous one crashes.
type Pool struct {
	tasks      chan<- task
	wg         sync.WaitGroup
	workerExec string
	mock       bool
	smgr       *stats.StatisticsMgr
}

// StartPool starts the receiver pool using the specified executable and number
// of payload submission workers.
func StartPool(workerExec string, numWorkers int, mock bool, smgr *stats.StatisticsMgr) (*Pool, error) {
	tasks := make(chan task)
	pool := &Pool{tasks, sync.WaitGroup{}, workerExec, mock, smgr}

	for i := 0; i < numWorkers; i++ {
		pool.wg.Add(1)
		go worker(tasks, pool, i)
	}

	gw.Log("worker_pool", gw.LogInfo).
		Msg("worker pool started")

	return pool, nil
}

// Stop all the background workers
func (p *Pool) Stop() error {
	close(p.tasks)
	p.wg.Wait()
	return nil
}

// SubmitPayload to be unpacked into the repository
func (p *Pool) SubmitPayload(ctx context.Context, leasePath string, payload io.Reader, digest string, headerSize int) error {
	reply := make(chan error, 1)
	p.tasks <- payloadTask{ctx, leasePath, payload, digest, headerSize, reply}
	result := <-reply
	return result
}

// CommitLease associated with the token (transaction commit)
func (p *Pool) CommitLease(ctx context.Context, leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag, directGraft bool) (uint64, error) {
	reply := make(chan error, 1)
	finalRevChan := make(chan uint64, 1)
	p.tasks <- commitTask{ctx, leasePath, oldRootHash, newRootHash, tag, directGraft, reply, finalRevChan}
	result := <-reply
	if result == nil {
		return <-finalRevChan, nil
	}
	return 0, result
}

func worker(tasks <-chan task, pool *Pool, workerIdx int) {
	gw.Log("worker_pool", gw.LogDebug).
		Int("worker_id", workerIdx).
		Msg("started")

	defer pool.wg.Done()

	// recv is the persistent receiver process for this worker.
	// nil means not yet started or crashed; ensureReceiver (re-)creates it.
	var recv Receiver

	// ensureReceiver starts a new receiver process if one is not running.
	ensureReceiver := func(ctx context.Context) error {
		if recv != nil {
			return nil
		}
		var err error
		recv, err = NewReceiver(ctx, pool.workerExec, pool.mock, pool.smgr)
		if err != nil {
			return fmt.Errorf("could not start receiver process: %w", err)
		}
		gw.LogC(ctx, "worker_pool", gw.LogInfo).
			Int("worker_id", workerIdx).
			Msg("receiver process started")
		return nil
	}

	// discardReceiver shuts down the current receiver (best-effort) and sets
	// recv to nil so ensureReceiver creates a fresh one on the next task.
	discardReceiver := func(ctx context.Context) {
		if recv == nil {
			return
		}
		if err := recv.Quit(); err != nil {
			gw.LogC(ctx, "worker_pool", gw.LogError).
				Int("worker_id", workerIdx).
				Msgf("error quitting receiver: %v", err)
		}
		recv = nil
	}

	defer func() {
		// Pool is stopping: cleanly quit the persistent receiver process.
		if recv != nil {
			if err := recv.Quit(); err != nil {
				gw.Log("worker_pool", gw.LogError).
					Int("worker_id", workerIdx).
					Msgf("error quitting receiver on shutdown: %v", err)
			}
		}
	}()

M:
	for {
		task, more := <-tasks

		if !more {
			break M
		}

		func() {
			t0 := time.Now()

			if err := ensureReceiver(task.Context()); err != nil {
				task.Reply() <- err
				close(task.Reply())
				return
			}

			var taskType string
			var result error
			var finalRev uint64
			var crashed bool

			switch t := task.(type) {
			case payloadTask:
				result = recv.SubmitPayload(t.leasePath, t.payload, t.digest, t.headerSize)
				taskType = "payload"
			case commitTask:
				finalRev, result = recv.Commit(t.leasePath, t.oldRootHash, t.newRootHash, t.tag, t.directGraft)
				taskType = "commit"
				t.finalRevChan <- finalRev
				close(t.finalRevChan)
			case testCrashTask:
				result = recv.TestCrash()
				taskType = "testcrash"
				// TestCrash always terminates the receiver process; replace next time.
				crashed = true
			default:
				task.Reply() <- fmt.Errorf("unknown task type")
				close(task.Reply())
				return
			}

			// Detect receiver process death. Application-level errors from the
			// receiver arrive as type receiver.Error (a string alias). I/O errors
			// from a crashed process are a different type.  On crash, discard the
			// receiver so a new one is created on the next task.
			if result != nil && !crashed {
				var appErr Error
				if !errors.As(result, &appErr) {
					gw.LogC(task.Context(), "worker_pool", gw.LogError).
						Int("worker_id", workerIdx).
						Str("task_type", taskType).
						Msgf("receiver process died, will restart on next task: %v", result)
					crashed = true
				}
			}
			if crashed {
				discardReceiver(task.Context())
			}

			task.Reply() <- result
			close(task.Reply())

			gw.LogC(task.Context(), "worker_pool", gw.LogDebug).
				Int("worker_id", workerIdx).
				Dur("task_dt", time.Since(t0)).
				Msgf("%v task complete", taskType)
		}()
	}

	gw.Log("worker_pool", gw.LogDebug).
		Int("worker_id", workerIdx).
		Msg("finished")
}
