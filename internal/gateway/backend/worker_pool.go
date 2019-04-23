package backend

import (
	"fmt"
	"sync"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// ReceiverTask is the common interface of all receiver tasks
type ReceiverTask interface {
	Reply() chan<- error
}

// PayloadTask is the input data for a payload submission task
type PayloadTask struct {
	leasePath  string
	payload    []byte
	digest     string
	headerSize int
	ReplyChan  chan<- error
}

// Reply returns the reply channel
func (p PayloadTask) Reply() chan<- error {
	return p.ReplyChan
}

// CommitTask is the input data for a commit task
type CommitTask struct {
	leasePath   string
	oldRootHash string
	newRootHash string
	tag         RepositoryTag
	ReplyChan   chan<- error
}

// Reply returns the reply channel
func (p CommitTask) Reply() chan<- error {
	return p.ReplyChan
}

// ReceiverPool maintains a number of parallel receiver workers to service
// payload submission and commit requests. Payload submissions are done in
// parallel, using Config.NumReceivers workers, while only a single commit
// request can be treated per repository at a time.
type ReceiverPool struct {
	tasks       chan<- ReceiverTask
	commitLocks sync.Map
	wg          *sync.WaitGroup
	workerExec  string
	mock        bool
}

// StartReceiverPool the receiver pool using the specified executable and number of payload
// submission workers
func StartReceiverPool(workerExec string, numWorkers int, mock bool) (*ReceiverPool, error) {
	// Start payload submission workers
	tasks := make(chan ReceiverTask)

	wg := &sync.WaitGroup{}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(tasks, wg, i, workerExec, mock)
	}

	gw.Log.Info().
		Str("component", "worker_pool").
		Msg("worker pool started")

	return &ReceiverPool{tasks, sync.Map{}, wg, workerExec, mock}, nil
}

// Stop all the background workers
func (p *ReceiverPool) Stop() error {
	close(p.tasks)
	p.wg.Wait()
	return nil
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

func worker(tasks <-chan ReceiverTask, wg *sync.WaitGroup, workerIdx int, workerExec string, mock bool) {
	gw.Log.Debug().
		Str("component", "worker_pool").
		Int("worker_id", workerIdx).
		Msg("started")

	defer wg.Done()
M:
	for {
		select {
		case task, more := <-tasks:
			receiver, err := NewReceiver(workerExec, mock)
			if err != nil {
				task.Reply() <- err
				continue M
			}

			var result error
			switch t := task.(type) {
			case PayloadTask:
				result = receiver.SubmitPayload(t.leasePath, t.payload, t.digest, t.headerSize)
			case CommitTask:
				result = receiver.Commit(t.leasePath, t.oldRootHash, t.newRootHash, t.tag)
			default:
				task.Reply() <- fmt.Errorf("unknown task type")
			}

			if err := receiver.Quit(); err != nil {
				task.Reply() <- err
				continue M
			}

			task.Reply() <- result

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
