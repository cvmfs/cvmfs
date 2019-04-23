package receiver

import (
	"fmt"
	"sync"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// task is the common interface of all receiver tasks
type task interface {
	Reply() chan<- error
}

// payloadTask is the input data for a payload submission task
type payloadTask struct {
	leasePath  string
	payload    []byte
	digest     string
	headerSize int
	replyChan  chan<- error
}

// Reply returns the reply channel
func (p payloadTask) Reply() chan<- error {
	return p.replyChan
}

// commitTask is the input data for a commit task
type commitTask struct {
	leasePath   string
	oldRootHash string
	newRootHash string
	tag         gw.RepositoryTag
	replyChan   chan<- error
}

// Reply returns the reply channel
func (p commitTask) Reply() chan<- error {
	return p.replyChan
}

// Pool maintains a number of parallel receiver workers to service
// payload submission and commit requests. Payload submissions are done in
// parallel, using Config.NumReceivers workers, while only a single commit
// request can be treated per repository at a time.
type Pool struct {
	tasks       chan<- task
	commitLocks sync.Map
	wg          sync.WaitGroup
	workerExec  string
	mock        bool
}

// StartPool the receiver pool using the specified executable and number of payload
// submission workers
func StartPool(workerExec string, numWorkers int, mock bool) (*Pool, error) {
	// Start payload submission workers
	tasks := make(chan task)

	pool := &Pool{tasks, sync.Map{}, sync.WaitGroup{}, workerExec, mock}

	for i := 0; i < numWorkers; i++ {
		pool.wg.Add(1)
		go worker(tasks, pool, i)
	}

	gw.Log.Info().
		Str("component", "worker_pool").
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
func (p *Pool) SubmitPayload(leasePath string, payload []byte, digest string, headerSize int) error {
	reply := make(chan error)
	p.tasks <- payloadTask{leasePath, payload, digest, headerSize, reply}
	result := <-reply
	return result
}

// CommitLease associated with the token (transaction commit)
// TODO: implement timeout or context?
func (p *Pool) CommitLease(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) error {
	reply := make(chan error)
	p.tasks <- commitTask{leasePath, oldRootHash, newRootHash, tag, reply}
	result := <-reply
	return result
}

/*
func (p *ReceiverPool) startCommitWorkerIfNeeded(repository string) {
	if _, ok := p.commits[repository]; !ok {
		cmt := make(chan CommitTask)
		go commitWorker(cmt, fmt.Sprintf("commit - %v", repository), p.workerExec, p.mock)
		p.commits[repository] = cmt
	}
}
*/

func worker(tasks <-chan task, pool *Pool, workerIdx int) {
	gw.Log.Debug().
		Str("component", "worker_pool").
		Int("worker_id", workerIdx).
		Msg("started")

	defer pool.wg.Done()
M:
	for {
		select {
		case task, more := <-tasks:
			t0 := time.Now()
			receiver, err := NewReceiver(pool.workerExec, pool.mock)
			if err != nil {
				task.Reply() <- err
				continue M
			}

			var taskType string
			var result error
			switch t := task.(type) {
			case payloadTask:
				result = receiver.SubmitPayload(t.leasePath, t.payload, t.digest, t.headerSize)
				taskType = "payload"
			case commitTask:
				repository, _, err := gw.SplitLeasePath(t.leasePath)
				if err != nil {
					task.Reply() <- err
					continue M
				}
				// Acquire commit lock for the repository
				m, _ := pool.commitLocks.LoadOrStore(repository, &sync.Mutex{})
				mtx := m.(*sync.Mutex)
				mtx.Lock()
				result = receiver.Commit(t.leasePath, t.oldRootHash, t.newRootHash, t.tag)
				mtx.Unlock()
				taskType = "commit"
			default:
				task.Reply() <- fmt.Errorf("unknown task type")
			}

			if err := receiver.Quit(); err != nil {
				task.Reply() <- err
				continue M
			}

			task.Reply() <- result

			gw.Log.Debug().
				Str("component", "worker_pool").
				Int("worker_id", workerIdx).
				Float64("time", time.Now().Sub(t0).Seconds()).
				Msgf("%v task complete", taskType)

			if !more {
				break M
			}
		}
	}

	gw.Log.Debug().
		Str("component", "worker_pool").
		Int("worker_id", workerIdx).
		Msg("finished")
}
