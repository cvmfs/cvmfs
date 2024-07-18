package db

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

const taskSqlFields string = "id, type, status, title, result, created_timestamp, start_timestamp, done_timestamp"
const taskSqlFieldsPrefixed string = "tasks.id, tasks.type, tasks.status, tasks.title, tasks.result, tasks.created_timestamp, tasks.start_timestamp, tasks.done_timestamp"
const taskSqlFieldsQs string = "?, ?, ?, ?, ?, ?, ?, ?"
const taskLogSqlFields string = "task_id, severity, message, timestamp"
const taskLogSqlFieldsQs string = "?, ?, ?, ?"
const taskRelationSqlFields string = "task1_id, task2_id, relation"
const taskRelationSqlFieldsQs string = "?, ?, ?"
const taskLogQueryFields string = "severity, message, timestamp"
const taskLogQueryQs string = "?, ?, ?"

type TaskID = uuid.UUID
type TaskType string

const (
	TASK_UPDATE_WISH  TaskType = "UPDATE_WISH"
	TASK_UPDATE_IMAGE TaskType = "UPDATE IMAGE"

	TASK_UPDATE_IMAGE_IN_REPO TaskType = "UPDATE_IMAGE_IN_REPO"

	// The main product tasks:
	TASK_CREATE_FLAT   TaskType = "CREATE_FLAT"
	TASK_CREATE_LAYERS TaskType = "CREATE_LAYERS"
	TASK_CREATE_PODMAN TaskType = "CREATE_PODMAN"
	TASK_CREATE_THIN   TaskType = "CREATE_THIN"

	// CREATE_LAYERS needs the following steps:
	TASK_CREATE_LAYER TaskType = "CREATE_LAYER"
	TASK_INGEST_LAYER TaskType = "INGEST_LAYER"
	CREATE_IMAGE_DATA TaskType = "CREATE_IMAGE_DATA"

	// CREATE_FLAT needs the following steps:
	TASK_CREATE_CHAIN             TaskType = "CREATE_CHAIN"
	TASK_CREATE_CHAIN_LINK        TaskType = "CREATE_CHAIN_LINK"
	TASK_INGEST_CHAIN_LINK        TaskType = "INGEST_CHAIN_LINK"
	TASK_CREATE_SINGULARITY_FILES TaskType = "CREATE_SINGULARITY_FILES"
	TASK_FETCH_OCI_CONFIG         TaskType = "FETCH_OCI_CONFIG"

	// This is used by both CREATE_LAYERS and CREATE_FLAT
	TASK_DOWNLOAD_BLOB TaskType = "DOWNLOAD_BLOB"

	TASK_MISC TaskType = "MISC"
)

type TaskStatus string

const (
	TASK_STATUS_NONE    TaskStatus = ""
	TASK_STATUS_PENDING TaskStatus = "PENDING"
	TASK_STATUS_RUNNING TaskStatus = "RUNNING"
	TASK_STATUS_DONE    TaskStatus = "DONE"
)

type TaskResult string

const (
	TASK_RESULT_NONE      TaskResult = ""
	TASK_RESULT_SUCCESS   TaskResult = "SUCCESS"
	TASK_RESULT_FAILURE   TaskResult = "FAILURE"
	TASK_RESULT_SKIPPED   TaskResult = "SKIPPED"
	TASK_RESULT_CANCELLED TaskResult = "CANCELLED"
	TASK_RESULT_ABORTED   TaskResult = "ABORTED"
)

type TaskRelation string

const (
	TASK_RELATION_SUBTASK_OF TaskRelation = "SUBTASK_OF"
)

type Task struct {
	ID       TaskID
	Type     TaskType
	Status   TaskStatus
	Title    string
	Result   TaskResult
	Subtasks []TaskPtr

	Input   any
	IsInput bool

	Artifact   any
	IsArtifact bool

	cv *sync.Cond
}

type TaskPtr struct {
	task *Task
	cv   *sync.Cond
}
type SmallTaskSnapshot struct {
	ID               TaskID
	Type             TaskType
	Status           TaskStatus
	CreatedTimestamp time.Time
	StartTimestamp   time.Time
	DoneTimestamp    time.Time
	Title            string
	Result           TaskResult
}

type FullTaskSnapshot struct {
	SmallTaskSnapshot
	SubtaskIDs []TaskID
	Logs       []Log
}

func NullTaskPtr() TaskPtr {
	return TaskPtr{
		task: nil,
		cv:   nil,
	}
}

func (t TaskPtr) GetValue() Task {
	t.cv.L.Lock()
	defer t.cv.L.Unlock()
	return *t.task
}

type Log struct {
	Severity  LogSeverity
	Message   string
	Timestamp time.Time
}

type LogSeverity int

const (
	LOG_SEVERITY_DEBUG LogSeverity = 4
	LOG_SEVERITY_INFO  LogSeverity = 3
	LOG_SEVERITY_WARN  LogSeverity = 2
	LOG_SEVERITY_ERROR LogSeverity = 1
	LOG_SEVERITY_FATAL LogSeverity = 0
)

// TaskResultSuccessful true if the [TaskResult] is considered successful
func TaskResultSuccessful(result TaskResult) bool {
	return result == TASK_RESULT_SUCCESS || result == TASK_RESULT_SKIPPED
}

// CreateTask creates a new task and returns two pointers to it.
// The *Task pointer is used to modify the task itself, only to be used by
// the corresponding task goroutine. Modifying the task struct directly is not thread safe.
// The TaskPtr can be used by other parts of the code, and all its methods are thread safe.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func CreateTask(tx *sql.Tx, taskType TaskType, title string) (*Task, TaskPtr, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, TaskPtr{}, err
		}
		defer tx.Rollback()
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return nil, TaskPtr{}, err
	}

	newTask := Task{
		ID:       id,
		Type:     taskType,
		Status:   TASK_STATUS_NONE,
		Title:    title,
		Result:   TASK_RESULT_NONE,
		Subtasks: make([]TaskPtr, 0),
		cv:       sync.NewCond(&sync.Mutex{}),
	}

	stnmt := "INSERT INTO tasks (" + taskSqlFields + ") VALUES (" + taskSqlFieldsQs + ")"
	_, err = tx.Exec(stnmt, newTask.ID, newTask.Type, newTask.Status, newTask.Title, newTask.Result, ToDBTimeStamp(time.Now()), sql.NullString{Valid: false}, sql.NullString{Valid: false})
	if err != nil {
		return nil, TaskPtr{}, err
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, TaskPtr{}, err
		}
	}

	ptr := TaskPtr{
		task: &newTask,
		cv:   newTask.cv,
	}

	return &newTask, ptr, nil
}

// SetTaskStatus sets the status of the task. For setting the status to done, use SetTaskCompleted instead.
// This method is thread safe, by locking the task CV mutex. Be careful of recursive locks.
// It will broadcast on the task CV, so any goroutine waiting on the CV will be woken up.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func (t *Task) SetTaskStatus(tx *sql.Tx, status TaskStatus) error {
	t.cv.L.Lock()
	defer t.cv.L.Unlock()
	t.cv.Broadcast()
	return t.setTaskStatusWithoutLocking(tx, status)
}

// setTaskStatusWithoutLocking is used internally to set the task status without locking the task CV mutex.
// Note that this is not thread safe, and should only be used by methods that already lock the task CV mutex.
// It does NOT broadcast on the task CV.
func (t *Task) setTaskStatusWithoutLocking(tx *sql.Tx, status TaskStatus) error {
	if status == TASK_STATUS_DONE {
		// Must set the result when setting the status to done
		return errors.New("cannot set status to done without setting the result")
	}
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	stnmt := "UPDATE tasks SET status=? WHERE id=?"
	_, err := tx.Exec(stnmt, status, t.ID)
	if err != nil {
		return err
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}

	t.Status = status
	return nil
}

// SetTaskCompleted sets the status of the task to done and sets the result to the given value.
// This method is thread safe, by locking the task CV mutex. Be careful of recursive locks.
// It will broadcast on the task CV, so any goroutine waiting on the CV will be woken up.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func (t *Task) SetTaskCompleted(tx *sql.Tx, result TaskResult) error {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	t.cv.L.Lock()
	defer t.cv.L.Unlock()

	stnmt := "UPDATE tasks SET status=?, result=?, done_timestamp=? WHERE id=?"
	_, err := tx.Exec(stnmt, TASK_STATUS_DONE, result, ToDBTimeStamp(time.Now()), t.ID)
	if err != nil {
		return fmt.Errorf("error setting task completed: %w", err)
	}

	// We don't want to leave any subtask running, unless they are children of another uncompleted task
	for _, subtask := range t.Subtasks {
		isAbandoned, err := taskIsAbandoned(tx, subtask.GetValue().ID)
		if err != nil {
			return fmt.Errorf("error checking if subtask is abandoned: %w", err)
		}
		if isAbandoned {
			// The subtask is abandoned, so we should cancel it
			err := subtask.Cancel(tx)
			if err != nil {
				return fmt.Errorf("error cancelling subtask: %w", err)
			}
		}
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}

	t.Result = result
	t.Status = TASK_STATUS_DONE
	t.cv.Broadcast()
	return nil
}

func taskIsAbandoned(tx *sql.Tx, taskID TaskID) (bool, error) {
	// Since we are only reading data, we don't need to commit the transaction.
	if tx == nil {
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
	}

	// Is the task done?
	{
		const stmnt string = "SELECT COUNT(*) FROM tasks WHERE id=? AND status=?"
		var count int
		if err := tx.QueryRow(stmnt, taskID, TASK_STATUS_DONE).Scan(&count); err != nil {
			return false, fmt.Errorf("error checking if the task is done: %w", err)
		}
		if count > 0 {
			// The task is done, so it is not abandoned
			return false, nil
		}
	}

	// Get the count of parents that are not done
	{
		const stmnt string = "SELECT COUNT(*) FROM task_relations JOIN tasks ON task2_id=id WHERE task1_id=? AND relation=? AND status!=?"
		var count int
		if err := tx.QueryRow(stmnt, taskID, TASK_RELATION_SUBTASK_OF, TASK_STATUS_DONE).Scan(&count); err != nil {
			return false, fmt.Errorf("error getting count of non-done parents: %w", err)
		}
		if count == 0 {
			return true, nil
		}
		return false, nil
	}
}

func SetTaskStatusById(tx *sql.Tx, taskID TaskID, status TaskStatus) error {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	stnmt := "UPDATE tasks SET status=? WHERE id=?"
	_, err := tx.Exec(stnmt, status, taskID)

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}
	return err
}

func SetTaskResultById(tx *sql.Tx, taskID TaskID, result TaskResult) error {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	stnmt := "UPDATE tasks SET result=? WHERE id=?"
	_, err := tx.Exec(stnmt, result, taskID)

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}
	return err
}

func AppendLogToTaskByID(tx *sql.Tx, taskID TaskID, severity LogSeverity, message string) error {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	const stmnt string = "INSERT INTO task_logs (" + taskLogSqlFields + ") VALUES (" + taskLogSqlFieldsQs + ")"
	timeStamp := ToDBTimeStamp(time.Now())

	_, err := tx.Exec(stmnt, taskID, severity, message, timeStamp)
	if err != nil {
		return err
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

// Log adds a log entry to the task.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func (t *Task) Log(tx *sql.Tx, severity LogSeverity, message string) error {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	timeStamp := ToDBTimeStamp(time.Now())

	stnmt := "INSERT INTO task_logs (" + taskLogSqlFields + ") VALUES (" + taskLogSqlFieldsQs + ")"
	_, err := tx.Exec(stnmt, t.ID, severity, message, timeStamp)
	if err != nil {
		return err
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

// LinkSubtask links the given task as a subtask of this task.
// This method is thread safe, by locking the task CV mutex. Be careful of recursive locks.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func (t *Task) LinkSubtask(tx *sql.Tx, child TaskPtr) error {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	t.cv.L.Lock()
	defer t.cv.L.Unlock()

	// First, check if the exact same relation already exists
	stmntCHeck := "SELECT COUNT(*) FROM task_relations WHERE task1_id=? AND task2_id=? AND relation=?"
	var count int
	if err := tx.QueryRow(stmntCHeck, child.GetValue().ID, t.ID, TASK_RELATION_SUBTASK_OF).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		// This exact relation already exists, so we don't need to do anything
		return nil
	}

	stmntInsert := "INSERT INTO task_relations (" + taskRelationSqlFields + ") VALUES (" + taskRelationSqlFieldsQs + ")"
	childID := child.GetValue().ID
	_, err := tx.Exec(stmntInsert, childID, t.ID, TASK_RELATION_SUBTASK_OF)
	if err != nil {
		return err
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}

	t.Subtasks = append(t.Subtasks, child)
	return nil
}

// LogFatal adds a log entry with fatal severity to the task and sets the task status to done with the given result.
// A call to SetTaskCompleted is made, which in turn locks the task CV mutex. Be careful of recursive locks.
// This method is thread safe.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func (t *Task) LogFatal(tx *sql.Tx, message string) error {
	t.Log(tx, LOG_SEVERITY_FATAL, message)
	return t.SetTaskCompleted(tx, TASK_RESULT_FAILURE)
}

// WaitUntilDone blocks until the task has been completed.
func (tPtr TaskPtr) WaitUntilDone() TaskResult {
	tPtr.cv.L.Lock()
	defer tPtr.cv.L.Unlock()
	for tPtr.task.Status != TASK_STATUS_DONE {
		tPtr.cv.Wait()
	}
	return tPtr.task.Result
}

// WaitForStart blocks until Task.Start() has been called.
// If false is returned, the task should not be started.
func (t *Task) WaitForStart() bool {
	t.cv.L.Lock()
	defer t.cv.L.Unlock()
	for t.Status == TASK_STATUS_NONE {
		t.cv.Wait()
	}
	if t.Status == TASK_STATUS_DONE {
		return false
	}
	t.setTaskStatusWithoutLocking(nil, TASK_STATUS_RUNNING)
	setStartTimestamp(nil, t.ID)
	return true
}

// Start starts the task, and returns the status of the task.
// If the task status was TASK_STATUS_NONE, it will be set to TASK_STATUS_PENDING.
// If the task has already been started, this is a no-op.
// This method is thread safe, by locking the task CV mutex. Be careful of recursive locks.
// It will broadcast on the task CV, so any goroutine waiting on the CV will be woken up.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func (tPtr TaskPtr) Start(tx *sql.Tx) (TaskStatus, error) {
	tPtr.cv.L.Lock()
	defer tPtr.cv.L.Unlock()
	if tPtr.task.Status != TASK_STATUS_NONE {
		return tPtr.task.Status, nil
	}

	var err error
	tPtr.task.setTaskStatusWithoutLocking(tx, TASK_STATUS_PENDING)
	if err != nil {
		return "", err
	}
	// We call setStartTimestamp in task.WaitForStart(), not here

	tPtr.cv.Broadcast()
	return tPtr.task.Status, nil
}

func setStartTimestamp(tx *sql.Tx, taskID TaskID) error {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	stmnt := "UPDATE tasks SET start_timestamp=? WHERE id=?"
	_, err := tx.Exec(stmnt, ToDBTimeStamp(time.Now()), taskID)

	if ownTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return err
}

func (tPtr TaskPtr) StartWithInput(tx *sql.Tx, input any) (TaskStatus, error) {
	tPtr.cv.L.Lock()
	defer tPtr.cv.L.Unlock()
	if tPtr.task.Status != TASK_STATUS_NONE {
		return tPtr.task.Status, nil
	}

	if tPtr.task.IsInput {
		return "", errors.New("task already has an input")
	}

	tPtr.task.Input = input

	var err error
	tPtr.task.setTaskStatusWithoutLocking(tx, TASK_STATUS_PENDING)
	if err != nil {
		return "", err
	}
	tPtr.cv.Broadcast()
	return tPtr.task.Status, nil
}

// Skips starts the task, and returns the status of the task.
// If the task status was TASK_STATUS_NONE, it will be set to TASK_STATUS_SKIPPED.
// If the task has already been started, this is a no-op.
// This method is thread safe, by locking the task CV mutex. Be careful of recursive locks.
// It will broadcast on the task CV, so any goroutine waiting on the CV will be woken up.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func (tPtr TaskPtr) Skip(tx *sql.Tx) (TaskStatus, error) {
	tPtr.cv.L.Lock()
	defer tPtr.cv.L.Unlock()
	if tPtr.task.Status != TASK_STATUS_NONE {
		return tPtr.task.Status, nil
	}

	var err error
	tPtr.task.setTaskStatusWithoutLocking(tx, TASK_STATUS_DONE)
	if err != nil {
		return "", err
	}
	tPtr.cv.Broadcast()
	return tPtr.task.Status, nil
}

func (tPtr TaskPtr) GetArtifact() (any, error) {
	tPtr.cv.L.Lock()
	defer tPtr.cv.L.Unlock()
	if tPtr.task.IsArtifact {
		return tPtr.task.Artifact, nil
	}
	return nil, errors.New("task does not have an artifact")
}

func (t *Task) SetArtifact(artifact any) error {
	t.cv.L.Lock()
	defer t.cv.L.Unlock()
	if t.IsArtifact {
		return errors.New("task already has an artifact")
	}
	t.Artifact = artifact
	t.IsArtifact = true
	return nil
}

func (t *Task) GetInput() (any, error) {
	t.cv.L.Lock()
	defer t.cv.L.Unlock()
	if t.IsInput {
		return t.Input, nil
	}
	return nil, errors.New("task does not have an input")
}

// GetTaskSnapshot returns a snapshot of the task with the given ID.
// If no task with the given ID exists, an sql.ErrNoRows is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetTaskSnapshotByID(tx *sql.Tx, taskID TaskID) (FullTaskSnapshot, error) {
	tasks, err := GetTaskSnapshotsByIDs(tx, []TaskID{taskID})
	if err != nil {
		return FullTaskSnapshot{}, err
	}
	return tasks[0], nil
}

// GetTaskSnapshotsByIDs returns a slice of snapshots of the tasks with the given IDs.
// Unless all tasks are found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetTaskSnapshotsByIDs(tx *sql.Tx, taskIDs []TaskID) ([]FullTaskSnapshot, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	taskStmnt := "SELECT " + taskSqlFields + " FROM tasks WHERE id = ?"
	taskPrepStmnt, err := tx.Prepare(taskStmnt)
	if err != nil {
		return nil, err
	}

	logsStmnt := "SELECT " + taskLogQueryFields + " FROM task_logs WHERE task_id = ? ORDER BY timestamp ASC"
	logsPrepStmnt, err := tx.Prepare(logsStmnt)
	if err != nil {
		return nil, err
	}

	subTasksStmnt := "SELECT task1_id FROM task_relations WHERE task2_id = ? AND relation = ?"
	subTasksPrepStmnt, err := tx.Prepare(subTasksStmnt)
	if err != nil {
		return nil, err
	}
	defer subTasksPrepStmnt.Close()

	taskSnapshots := make([]FullTaskSnapshot, len(taskIDs))
	for i, taskID := range taskIDs {
		var snapshot FullTaskSnapshot
		// Get the task itself
		taskRow := taskPrepStmnt.QueryRow(taskID)
		if err != nil {
			return nil, err
		}
		snapshot.SmallTaskSnapshot, err = parseSmallTaskSnapshotFromRow(taskRow)
		if err != nil {
			return nil, err
		}

		// Get the logs and append them to the snapshot
		logsRows, err := logsPrepStmnt.Query(taskID)
		if err != nil {
			return nil, err
		}
		logs := make([]Log, 0)
		for logsRows.Next() {
			log, err := parseLogFromRow(logsRows)
			if err != nil {
				logsRows.Close()
				return nil, err
			}
			logs = append(logs, log)
		}
		snapshot.Logs = logs

		// Get the subtask IDs and append them to the snapshot
		subTaskRows, err := subTasksPrepStmnt.Query(taskID, TASK_RELATION_SUBTASK_OF)
		if err != nil {
			return nil, err
		}

		subTaskIDs := make([]TaskID, 0)
		for subTaskRows.Next() {
			var subTaskID TaskID
			err := subTaskRows.Scan(&subTaskID)
			if err != nil {
				subTaskRows.Close()
				return nil, err
			}
			subTaskIDs = append(subTaskIDs, subTaskID)
		}
		snapshot.SubtaskIDs = subTaskIDs

		taskSnapshots[i] = snapshot
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return taskSnapshots, nil
}

// GetLogByTaskID returns the logs of the task with the given ID.
// If no task with the given ID exists, an sql.ErrNoRows is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetLogByTaskID(tx *sql.Tx, taskID TaskID) ([]Log, error) {
	logs, err := GetLogsByTaskIDs(tx, []TaskID{taskID})
	if err != nil {
		return nil, err
	}
	return logs[0], nil
}

// GetLogsByTaskIDs returns the logs of the tasks with the given IDs.
// Unless all tasks are found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetLogsByTaskIDs(tx *sql.Tx, taskIDs []TaskID) ([][]Log, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	stmnt := "SELECT " + taskLogQueryFields + " FROM task_logs WHERE task_id = ? ORDER BY timestamp ASC"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	logs := make([][]Log, len(taskIDs))
	for i, taskID := range taskIDs {
		rows, err := prepStmnt.Query(taskID)
		if err != nil {
			return nil, err
		}

		logs[i] = make([]Log, 0)
		for rows.Next() {
			log, err := parseLogFromRow(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			logs[i] = append(logs[i], log)
		}
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return logs, nil
}

// parseLogFromRow parses a log from a row returned by a query.
// The row must contain the fields of logSqlQueryFields, in the same order.
func parseLogFromRow(row scannableRow) (Log, error) {
	var log Log
	var timestampStr string

	err := row.Scan(&log.Severity, &log.Message, &timestampStr)
	if err != nil {
		return Log{}, err
	}

	log.Timestamp, err = FromDBTimeStamp(timestampStr)
	if err != nil {
		return Log{}, err
	}

	return log, nil
}

func parseSmallTaskSnapshotFromRow(row scannableRow) (SmallTaskSnapshot, error) {
	var taskSnapshot SmallTaskSnapshot

	var startTimestampStr, doneTimestampStr sql.NullString
	var createdTimestampStr string

	err := row.Scan(&taskSnapshot.ID, &taskSnapshot.Type, &taskSnapshot.Status, &taskSnapshot.Title, &taskSnapshot.Result, &createdTimestampStr, &startTimestampStr, &doneTimestampStr)
	if err != nil {
		return SmallTaskSnapshot{}, err
	}

	taskSnapshot.CreatedTimestamp, err = FromDBTimeStamp(createdTimestampStr)
	if err != nil {
		return SmallTaskSnapshot{}, fmt.Errorf("invalid timestamp: %v", err)
	}

	if startTimestampStr.Valid {
		taskSnapshot.StartTimestamp, err = FromDBTimeStamp(startTimestampStr.String)
		if err != nil {
			return SmallTaskSnapshot{}, fmt.Errorf("invalid timestamp: %v", err)
		}
	}

	if doneTimestampStr.Valid {
		taskSnapshot.DoneTimestamp, err = FromDBTimeStamp(doneTimestampStr.String)
		if err != nil {
			return SmallTaskSnapshot{}, fmt.Errorf("invalid timestamp: %v", err)
		}
	}

	return taskSnapshot, nil
}

func (tPtr TaskPtr) Cancel(tx *sql.Tx) error {
	return tPtr.task.SetTaskCompleted(tx, TASK_RESULT_CANCELLED)
}

func GetTasksByStatusAndType(tx *sql.Tx, statusFilter []TaskStatus, typeFilter []TaskType) ([]SmallTaskSnapshot, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	// Reserve space for ID
	var args = make([]any, 1)

	typeQuery := ""
	if len(typeFilter) > 0 {
		typeQuery = " AND type IN ("
		for i, checkType := range typeFilter {
			if i > 0 {
				typeQuery += ","
			}
			typeQuery += "?"
			args = append(args, checkType)
		}
		typeQuery += ")"
	}

	statusQuery := ""
	if len(statusFilter) > 0 {
		statusQuery = " AND status IN ("
		for i, statusType := range statusFilter {
			if i > 0 {
				statusQuery += ","
			}
			statusQuery += "?"
			args = append(args, statusType)
		}
		statusQuery += ")"
	}

	stmnt := "SELECT " + taskSqlFields + " FROM tasks" + typeQuery + statusQuery + "ORDER BY start_timestamp DESC"

	rows, err := tx.Query(stmnt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]SmallTaskSnapshot, 0)
	for rows.Next() {
		check, err := parseSmallTaskSnapshotFromRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, check)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

func GetRootTasks(tx *sql.Tx) ([]SmallTaskSnapshot, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	stmnt := "SELECT " + taskSqlFields + " FROM tasks WHERE id NOT IN (SELECT task1_id FROM task_relations WHERE relation = ?)"
	rows, err := tx.Query(stmnt, TASK_RELATION_SUBTASK_OF)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]SmallTaskSnapshot, 0)
	for rows.Next() {
		check, err := parseSmallTaskSnapshotFromRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, check)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

func GetAllUnfinishedTasks(tx *sql.Tx) ([]SmallTaskSnapshot, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	stmnt := "SELECT " + taskSqlFields + " FROM tasks WHERE status != ?"
	rows, err := tx.Query(stmnt, TASK_STATUS_DONE)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]SmallTaskSnapshot, 0)
	for rows.Next() {
		check, err := parseSmallTaskSnapshotFromRow(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, check)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func GetAllUnfinishedRootTasks(tx *sql.Tx) ([]SmallTaskSnapshot, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	stmnt := "SELECT " + taskSqlFields + " FROM tasks WHERE status != ? AND id NOT IN (SELECT task1_id FROM task_relations WHERE relation = ?)"
	rows, err := tx.Query(stmnt, TASK_STATUS_DONE, TASK_RELATION_SUBTASK_OF)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]SmallTaskSnapshot, 0)
	for rows.Next() {
		check, err := parseSmallTaskSnapshotFromRow(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, check)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}
