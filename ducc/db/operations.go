package db

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
)

const checkSqlFields string = "id, type, entity_id, status, task_id"
const checkSqlFieldsQs string = "?,?,?,?,?"

const checkTriggerSqlFields string = "id, check_type, entity_id, type, timestamp, details, check_id"
const checkTriggerSqlFieldsQs string = "?,?,?,?,?,?,?"

type OperationType string

const (
	OP_TYPE_EXPAND_WILDCARDS OperationType = "EXPAND_WILDCARDS"
	OP_TYPE_UPDATE_IMAGE     OperationType = "UPDATE_IMAGE"
)

type CheckStatus string

const (
	OP_STATUS_RUNNING   CheckStatus = "RUNNING"
	OP_STATUS_SUCCESS   CheckStatus = "SUCCESS"
	CHECK_STATUS_FAILED CheckStatus = "FAILED"
)

type TriggerType string

const (
	TRIGGER_TYPE_MANUAL      TriggerType = "MANUAL"
	TRIGGER_TYPE_SCHEDULED   TriggerType = "SCHEDULED"
	TRIGGER_TYPE_NEW_IMAGE   TriggerType = "NEW_IMAGE"
	TRIGGER_TYPE_WISH_CHANGE TriggerType = "WISH_CHANGE"
	TRIGGER_TYPE_WEBHOOK     TriggerType = "WEBHOOK"
)

type CheckTrigger struct {
	ID uuid.UUID

	CheckType OperationType
	EntityID  uuid.UUID

	Type      TriggerType
	Timestamp time.Time
	Details   string

	CheckID uuid.UUID
}

type Check struct {
	ID uuid.UUID

	Type     OperationType
	EntityID uuid.UUID

	Status CheckStatus
	TaskID uuid.UUID

	DoneTimestamp time.Time
}

func CreateCheckTrigger(tx *sql.Tx, trigger CheckTrigger) (CheckTrigger, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return CheckTrigger{}, err
		}
		defer tx.Rollback()
		ownTx = true
	}

	stmnt := "INSERT INTO checkTriggers (" + checkTriggerSqlFields + ") VALUES (" + checkTriggerSqlFieldsQs + ") RETURNING " + checkTriggerSqlFields

	timestampStr := ToDBTimeStamp(trigger.Timestamp)
	checkID := sql.NullString{String: trigger.CheckID.String(), Valid: trigger.CheckID != uuid.Nil}
	// If no ID is provided, generate a new one
	if trigger.ID == uuid.Nil {
		trigger.ID = uuid.New()
	}
	row := tx.QueryRow(stmnt, trigger.ID, trigger.CheckType, trigger.EntityID, trigger.Type, timestampStr, trigger.Details, checkID)
	out, err := parseCheckTriggerFromRow(row)
	if err != nil {
		return CheckTrigger{}, err
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return CheckTrigger{}, err
		}
	}
	return out, nil

}

func CreateCheck(tx *sql.Tx, check Check) (Check, error) {
	checks, err := CreateChecks(tx, []Check{check})
	if err != nil {
		return Check{}, err
	}
	return checks[0], nil
}

func CreateChecks(tx *sql.Tx, checks []Check) ([]Check, error) {
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

	const stmnt string = "INSERT INTO checks (" + checkSqlFields + ") VALUES (" + checkSqlFieldsQs + ") RETURNING " + checkSqlFields
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	out := make([]Check, 0, len(checks))
	for _, check := range checks {
		// If no ID is provided, generate a new one
		if check.ID == uuid.Nil {
			check.ID = uuid.New()
		}
		row := prepStmnt.QueryRow(check.ID, check.Type, check.EntityID, check.Status, check.TaskID)
		check, err := parseCheckFromRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, check)
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func parseCheckFromRow(row scannableRow) (Check, error) {
	out := Check{}

	var taskIDStr sql.NullString

	err := row.Scan(&out.ID, &out.Type, &out.EntityID, &out.Status, &taskIDStr)
	if err != nil {
		return Check{}, err
	}
	// Check if the task ID is valid, and parse it if it is
	if taskIDStr.Valid {
		out.TaskID, err = uuid.Parse(taskIDStr.String)
		if err != nil {
			return Check{}, err
		}
	}
	return out, nil
}

func GetChecksByStatus(tx *sql.Tx, status CheckStatus) ([]Check, error) {
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

	const stmnt string = "SELECT " + checkSqlFields + " FROM checks WHERE status = ?"
	rows, err := tx.Query(stmnt, status)
	if err != nil {
		return nil, err
	}

	out := make([]Check, 0)
	for rows.Next() {
		check, err := parseCheckFromRow(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, check)
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func getChecksForEntityIdsFilterByTypeAndStatusInternal(tx *sql.Tx, entityIDs []uuid.UUID, typeFilter []OperationType, statusFilter []CheckStatus, limit int) ([][]Check, error) {
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

	limitQuery := ""
	if limit > 0 {
		limitQuery = " LIMIT ?"
		args = append(args, limit)
	}

	var stmnt string = "SELECT " + checkSqlFields + " FROM checks WHERE entity_id = ?" + typeQuery + statusQuery + " ORDER BY trigger_timestamp DESC" + limitQuery
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	out := make([][]Check, len(entityIDs))
	for i, entityId := range entityIDs {
		out[i] = make([]Check, 0)
		args[0] = entityId
		rows, err := prepStmnt.Query(args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			check, err := parseCheckFromRow(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			out[i] = append(out[i], check)
		}
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func GetChecksForEntityIDFilterByTypeAndStatus(tx *sql.Tx, entityID uuid.UUID, typeFilter []OperationType, statusFilter []CheckStatus) ([]Check, error) {
	checks, err := getChecksForEntityIdsFilterByTypeAndStatusInternal(tx, []uuid.UUID{entityID}, typeFilter, statusFilter, 0)
	if err != nil {
		return nil, err
	}
	return checks[0], nil
}

func GetChecksForEntityIDsFilterByTypeAndStatus(tx *sql.Tx, entityIDs []uuid.UUID, typeFilter []OperationType, statusFilter []CheckStatus) ([][]Check, error) {
	return getChecksForEntityIdsFilterByTypeAndStatusInternal(tx, entityIDs, typeFilter, statusFilter, 0)
}

func GetLastCheckForEntityIDFilterByTypeAndStatus(tx *sql.Tx, entityID uuid.UUID, typeFilter []OperationType, statusFilter []CheckStatus) (*Check, error) {
	checks, err := getChecksForEntityIdsFilterByTypeAndStatusInternal(tx, []uuid.UUID{entityID}, typeFilter, statusFilter, 1)
	if err != nil {
		return nil, err
	}
	if len(checks[0]) == 0 {
		return nil, nil
	}
	return &checks[0][0], nil
}

func GetLastChecksForEntityIDsFilterByTypeAndStatus(tx *sql.Tx, entityIDs []uuid.UUID, typeFilter []OperationType, statusFilter []CheckStatus) ([]*Check, error) {
	checks, err := getChecksForEntityIdsFilterByTypeAndStatusInternal(tx, entityIDs, typeFilter, statusFilter, 1)
	if err != nil {
		return nil, err
	}
	out := make([]*Check, len(checks))
	for i, check := range checks {
		if len(check) == 0 {
			out[i] = nil
		} else {
			out[i] = &check[0]
		}
	}
	return out, nil
}

func GetPendingCheckTriggersForEntityIDsFilterByType(tx *sql.Tx, entityIDs []uuid.UUID, typeFilter []OperationType) ([][]CheckTrigger, error) {
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

	var stmnt string = "SELECT " + checkTriggerSqlFields + " FROM checkTriggers WHERE entity_id = ? AND check_id IS NULL" + typeQuery + " ORDER BY trigger_timestamp DESC"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	out := make([][]CheckTrigger, len(entityIDs))
	for i, entityId := range entityIDs {
		out[i] = make([]CheckTrigger, 0)
		args[0] = entityId
		rows, err := prepStmnt.Query(args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			check, err := parseCheckTriggerFromRow(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			out[i] = append(out[i], check)
		}
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func GetPendingCheckTriggersForEntityIDFilterByType(tx *sql.Tx, entityID uuid.UUID, typeFilter []OperationType) ([]CheckTrigger, error) {
	checks, err := GetPendingCheckTriggersForEntityIDsFilterByType(tx, []uuid.UUID{entityID}, typeFilter)
	if err != nil {
		return nil, err
	}
	return checks[0], nil
}

func LinkTriggersToCheck(tx *sql.Tx, checkID uuid.UUID, triggerIDs []uuid.UUID) error {
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

	const stmnt string = "UPDATE checkTriggers SET check_id = ? WHERE id = ?"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return err
	}
	defer prepStmnt.Close()

	for _, triggerID := range triggerIDs {
		_, err := prepStmnt.Exec(checkID, triggerID)
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

func parseCheckTriggerFromRow(row scannableRow) (CheckTrigger, error) {
	out := CheckTrigger{}

	var checkIDStr sql.NullString

	err := row.Scan(&out.ID, &out.CheckType, &out.EntityID, &out.Type, &out.Timestamp, &out.Details, &checkIDStr)
	if err != nil {
		return CheckTrigger{}, err
	}
	// Check if the check ID is valid, and parse it if it is
	if checkIDStr.Valid {
		out.CheckID, err = uuid.Parse(checkIDStr.String)
		if err != nil {
			return CheckTrigger{}, err
		}
	}
	return out, nil
}

func UpdateStatusForCheckById(tx *sql.Tx, checkID uuid.UUID, status CheckStatus) (Check, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return Check{}, err
		}
		defer tx.Rollback()
		ownTx = true
	}

	const stmnt string = "UPDATE checks SET status = ? WHERE id = ? RETURNING " + checkSqlFields
	row := tx.QueryRow(stmnt, status, checkID)
	out, err := parseCheckFromRow(row)
	if err != nil {
		return Check{}, err
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return Check{}, err
		}
	}

	return out, nil
}

func GetPendingCheckTriggerTypesForEntityByID(tx *sql.Tx, entityID uuid.UUID) ([]OperationType, error) {
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

	const stmnt string = "SELECT DISTINCT type FROM checkTriggers WHERE entity_id = ? AND check_id IS NULL"
	rows, err := tx.Query(stmnt, entityID)
	if err != nil {
		return nil, err
	}

	out := make([]OperationType, 0)
	for rows.Next() {
		var checkType OperationType
		err := rows.Scan(&checkType)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, checkType)
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	return out, nil
}
