package backend

import (
	"fmt"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// PayloadTask is the input data for a payload submission task
type PayloadTask struct {
	leasePath  string
	payload    []byte
	digest     string
	headerSize int
	Reply      chan<- error
}

// CommitTask is the input data for a commit task
type CommitTask struct {
	leasePath   string
	oldRootHash string
	newRootHash string
	tag         RepositoryTag
	Reply       chan<- error
}

// WorkerPool maintains a number of parallel receiver workers to service
// payload submission and commit requests. Payload submissions are done in
// parallel, using Config.NumReceivers workers, while only a single commit
// request can be treated per repository at a time.
type WorkerPool struct {
	payloads chan<- PayloadTask
	// commitSubmission is a map from repository name -> submission channel
	commits    map[string]chan<- CommitTask
	workerExec string
	mock       bool
}

// StartWorkerPool the receiver pool using the specified executable and number of payload
// submission workers
func StartWorkerPool(workerExec string, numWorkers int, mock bool) (*WorkerPool, error) {
	// Start payload submission workers
	payloads := make(chan PayloadTask)

	for i := 0; i < numWorkers; i++ {
		go payloadWorker(payloads, fmt.Sprintf("payload - %v", i), workerExec, mock)
	}

	commits := make(map[string]chan<- CommitTask)

	gw.Log.Info().
		Str("component", "worker_pool").
		Msg("worker pool started")

	return &WorkerPool{payloads, commits, workerExec, mock}, nil
}

// Stop all the background workers
func (p *WorkerPool) Stop() error {
	close(p.payloads)
	for _, v := range p.commits {
		close(v)
	}
	return nil
}

func (p *WorkerPool) startCommitWorkerIfNeeded(repository string) {
	if _, ok := p.commits[repository]; !ok {
		cmt := make(chan CommitTask)
		go commitWorker(cmt, fmt.Sprintf("commit - %v", repository), p.workerExec, p.mock)
		p.commits[repository] = cmt
	}
}

func payloadWorker(tasks <-chan PayloadTask, workerID string, workerExec string, mock bool) {
	gw.Log.Debug().
		Str("component", "worker_pool").
		Str("worker_id", workerID).
		Msg("started")
M:
	for {
		select {
		case task, more := <-tasks:
			receiver, err := NewReceiver(workerExec, mock)
			if err != nil {
				task.Reply <- err
				continue M
			}

			result := receiver.SubmitPayload(task.leasePath, task.payload, task.digest, task.headerSize)

			if err := receiver.Quit(); err != nil {
				task.Reply <- err
				continue M
			}

			task.Reply <- result

			if !more {
				break M
			}
		}
	}

	gw.Log.Debug().
		Str("component", "worker_pool").
		Str("worker_id", workerID).
		Msg("finished")
}

func commitWorker(tasks <-chan CommitTask, workerID string, workerExec string, mock bool) {
	gw.Log.Debug().
		Str("component", "worker_pool").
		Str("worker_id", workerID).
		Msg("started")
M:
	for {
		select {
		case task, more := <-tasks:
			receiver, err := NewReceiver(workerExec, mock)
			if err != nil {
				task.Reply <- err
				continue M
			}

			result := receiver.Commit(task.leasePath, task.oldRootHash, task.newRootHash, task.tag)

			if err := receiver.Quit(); err != nil {
				task.Reply <- err
				continue M
			}

			task.Reply <- result

			if !more {
				break M
			}
		}
	}

	gw.Log.Debug().
		Str("component", "worker_pool").
		Str("worker_id", workerID).
		Msg("finished")
}
