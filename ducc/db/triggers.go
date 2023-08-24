package db

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
)

const triggerSqlFields string = "id, action, object_id, timestamp, reason, details, task_id"
const triggerSqlFieldsPrefixed string = "triggers.id, triggers.action, triggers.object_id, triggers.timestamp, triggers.reason, triggers.details, triggers.task_id"
const triggerSqlFieldsQs string = "?,?,?,?,?,?,?"

type TriggerType string

type Trigger struct {
	ID uuid.UUID

	Action   string
	ObjectID uuid.UUID

	Timestamp time.Time
	Reason    string
	Details   string

	TaskID uuid.UUID
}

func CreateTrigger(tx *sql.Tx, trigger Trigger) (Trigger, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return Trigger{}, err
		}
		defer tx.Rollback()
		ownTx = true
	}

	stmnt := "INSERT INTO triggers (" + triggerSqlFields + ") VALUES (" + triggerSqlFieldsQs + ") RETURNING " + triggerSqlFields

	timestampStr := ToDBTimeStamp(trigger.Timestamp)
	taskID := sql.NullString{String: trigger.TaskID.String(), Valid: trigger.TaskID != uuid.Nil}
	// If no ID is provided, generate a new one
	if trigger.ID == uuid.Nil {
		trigger.ID = uuid.New()
	}
	row := tx.QueryRow(stmnt, trigger.ID, trigger.Action, trigger.ObjectID, timestampStr, trigger.Reason, trigger.Details, taskID)
	out, err := parseTriggerFromRow(row)
	if err != nil {
		return Trigger{}, err
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return Trigger{}, err
		}
	}
	return out, nil
}

func GetTasksForObjectIDs(tx *sql.Tx, objectIDs []uuid.UUID, actionFilter []string, statusFilter []TaskStatus, limit int) ([][]SmallTaskSnapshot, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		ownTx = true
	}

	// Reserve space for ID
	var args = make([]any, 1)

	typeQuery := ""
	if len(actionFilter) > 0 {
		typeQuery = " AND triggers.action IN ("
		for i, action := range actionFilter {
			if i > 0 {
				typeQuery += ","
			}
			typeQuery += "?"
			args = append(args, action)
		}
		typeQuery += ")"
	}

	statusQuery := ""
	if len(statusFilter) > 0 {
		statusQuery = " AND tasks.status IN ("
		for i, statusType := range statusFilter {
			if i > 0 {
				statusQuery += ","
			}
			statusQuery += "?"
			args = append(args, statusType)
		}
		statusQuery += ")"
	}

	limitQuery := ""
	if limit > 0 {
		limitQuery = " LIMIT ?"
		args = append(args, limit)
	}

	stmnt := "SELECT " + taskSqlFieldsPrefixed + " FROM tasks JOIN triggers ON tasks.id = triggers.task_id WHERE triggers.object_id = ?" + typeQuery + statusQuery + " ORDER BY task.start_timestamp DESC" + limitQuery
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	out := make([][]SmallTaskSnapshot, len(objectIDs))
	for i, objectID := range objectIDs {
		out[i] = make([]SmallTaskSnapshot, 0)
		args[0] = objectID
		rows, err := prepStmnt.Query(args...)
		defer rows.Close()
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			TaskSnapshot, err := parseSmallTaskSnapshotFromRow(rows)
			if err != nil {
				return nil, err
			}
			out[i] = append(out[i], TaskSnapshot)
		}
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func GetTasksForObjectID(tx *sql.Tx, objectID uuid.UUID, actionFilter []string, statusFilter []TaskStatus, limit int) ([]SmallTaskSnapshot, error) {
	tasks, err := GetTasksForObjectIDs(tx, []uuid.UUID{objectID}, actionFilter, statusFilter, limit)
	if err != nil {
		return nil, err
	}
	return tasks[0], nil
}

func GetUnfinishedTriggers(tx *sql.Tx) ([]Trigger, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		ownTx = true
	}

	// We match all triggers that do not have a task ID, or have a task ID that is not done
	const stmnt string = "SELECT " + triggerSqlFields + " FROM triggers WHERE triggers.task_id IS NULL UNION SELECT " + triggerSqlFieldsPrefixed + " FROM tasks JOIN triggers ON tasks.id = triggers.task_id WHERE tasks.status != ?"

	rows, err := tx.Query(stmnt, TASK_STATUS_DONE)
	if err != nil {
		return nil, err
	}
	out := make([]Trigger, 0)
	for rows.Next() {
		trigger, err := parseTriggerFromRow(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, trigger)
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func GetPendingTriggersForObjectIDs(tx *sql.Tx, objectIDs []uuid.UUID, actionFilter []string) ([][]Trigger, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		ownTx = true
	}

	// Reserve space for ID
	var args = make([]any, 1)

	typeQuery := ""
	if len(actionFilter) > 0 {
		typeQuery = " AND action IN ("
		for i, action := range actionFilter {
			if i > 0 {
				typeQuery += ","
			}
			typeQuery += "?"
			args = append(args, action)
		}
		typeQuery += ")"
	}

	var stmnt string = "SELECT " + triggerSqlFields + " FROM triggers WHERE object_id = ? AND task_id IS NULL" + typeQuery + " ORDER BY timestamp DESC"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	out := make([][]Trigger, len(objectIDs))
	for i, objectID := range objectIDs {
		out[i] = make([]Trigger, 0)
		args[0] = objectID
		rows, err := prepStmnt.Query(args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			trigger, err := parseTriggerFromRow(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			out[i] = append(out[i], trigger)
		}
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func GetPendingTriggersForObjectID(tx *sql.Tx, objectID uuid.UUID, actionFilter []string) ([]Trigger, error) {
	triggers, err := GetPendingTriggersForObjectIDs(tx, []uuid.UUID{objectID}, actionFilter)
	if err != nil {
		return nil, err
	}
	return triggers[0], nil
}

func UnlinkTriggersFromTask(tx *sql.Tx, triggerIDs []uuid.UUID) error {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		ownTx = true
	}

	const stmnt string = "UPDATE triggers SET task_id = NULL WHERE id = ?"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return err
	}
	defer prepStmnt.Close()

	for _, triggerID := range triggerIDs {
		_, err := prepStmnt.Exec(triggerID)
		if err != nil {
			return err
		}
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func LinkTriggersToTask(tx *sql.Tx, taskID TaskID, triggerIDs []uuid.UUID) error {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		ownTx = true
	}

	const stmnt string = "UPDATE triggers SET task_id = ? WHERE id = ?"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return err
	}
	defer prepStmnt.Close()

	for _, triggerID := range triggerIDs {
		_, err := prepStmnt.Exec(taskID, triggerID)
		if err != nil {
			return err
		}
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func parseTriggerFromRow(row scannableRow) (Trigger, error) {
	out := Trigger{}

	var taskIDStr sql.NullString

	var timestampStr string

	err := row.Scan(&out.ID, &out.Action, &out.ObjectID, &timestampStr, &out.Reason, &out.Details, &taskIDStr)
	if err != nil {
		return Trigger{}, err
	}

	out.Timestamp, err = FromDBTimeStamp(timestampStr)
	if err != nil {
		return Trigger{}, err
	}

	// Check if the task ID is valid, and parse it if it is
	if taskIDStr.Valid {
		out.TaskID, err = uuid.Parse(taskIDStr.String)
		if err != nil {
			return Trigger{}, err
		}
	}
	return out, nil
}

func GetPendingActionsForObjectByID(tx *sql.Tx, objectID uuid.UUID) ([]string, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		ownTx = true
	}

	const stmnt string = "SELECT DISTINCT action FROM triggers WHERE object_id = ? AND task_id IS NULL"
	rows, err := tx.Query(stmnt, objectID)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0)
	for rows.Next() {
		var action string
		err := rows.Scan(&action)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, action)
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func GetPendingTriggers(tx *sql.Tx) ([]Trigger, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		ownTx = true
	}

	const stmnt string = "SELECT " + triggerSqlFields + " FROM triggers WHERE task_id IS NULL"
	rows, err := tx.Query(stmnt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Trigger, 0)
	for rows.Next() {
		trigger, err := parseTriggerFromRow(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, trigger)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	return out, nil
}
