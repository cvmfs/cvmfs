package receiver

import (
	"context"
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

// Pool maintains a number of parallel receiver workers to service
// payload submission and commit requests. Payload submissions are done in
// parallel, using Config.NumReceivers workers, while only a single commit
// request can be treated per repository at a time.
type Pool struct {
	tasks      chan<- task
	wg         sync.WaitGroup
	workerExec string
	mock       bool
	smgr       *stats.StatisticsMgr
}

// StartPool the receiver pool using the specified executable and number of payload
// submission workers
func StartPool(workerExec string, numWorkers int, mock bool, smgr *stats.StatisticsMgr) (*Pool, error) {
	// Start payload submission workers
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
// TODO: implement timeout or context?
func (p *Pool) SubmitPayload(ctx context.Context, leasePath string, payload io.Reader, digest string, headerSize int) error {
	reply := make(chan error, 1)
	p.tasks <- payloadTask{ctx, leasePath, payload, digest, headerSize, reply}
	result := <-reply
	return result
}

// CommitLease associated with the token (transaction commit)
// TODO: implement timeout or context?
func (p *Pool) CommitLease(ctx context.Context, leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) (uint64, error) {
	reply := make(chan error, 1)
	finalRevChan := make(chan uint64, 1)
	p.tasks <- commitTask{ctx, leasePath, oldRootHash, newRootHash, tag, reply, finalRevChan}
	result := <-reply
	if result == nil {
		return <-finalRevChan, nil
	}
	return 0, result
}

// this is private, it is not enough to just make it private but it is a good start
func (p *Pool) testCrash(ctx context.Context) error {
	reply := make(chan error)
	p.tasks <- testCrashTask{ctx, reply}
	result := <-reply
	return result
}

func worker(tasks <-chan task, pool *Pool, workerIdx int) {
	gw.Log("worker_pool", gw.LogDebug).
		Int("worker_id", workerIdx).
		Msg("started")

	defer pool.wg.Done()
M:
	for {
		task, more := <-tasks

		if !more {
			break M
		}

		func() {
			t0 := time.Now()
			receiver, err := NewReceiver(task.Context(), pool.workerExec, pool.mock, pool.smgr)
			if err != nil {
				task.Reply() <- err
				return
			}
			defer func() {
				if err := receiver.Quit(); err != nil {
					gw.LogC(task.Context(), "worker_pool", gw.LogError).
						Int("worker_id", workerIdx).
						Msgf("error when quitting the receiver: %v", err.Error())
				}
			}()

			var taskType string
			var result error
			var finalRev uint64
			switch t := task.(type) {
			case payloadTask:
				result = receiver.SubmitPayload(t.leasePath, t.payload, t.digest, t.headerSize)
				taskType = "payload"
			case commitTask:
				finalRev, result = receiver.Commit(t.leasePath, t.oldRootHash, t.newRootHash, t.tag)
				taskType = "commit"
				t.finalRevChan <- finalRev
				close(t.finalRevChan)
			case testCrashTask:
				result = receiver.TestCrash()
				taskType = "testcrash"
			default:
				task.Reply() <- fmt.Errorf("unknown task type")
				return
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
