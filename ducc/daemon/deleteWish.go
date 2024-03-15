package daemon

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cvmfs/ducc/db"
)

const DELETE_WISH_ACTION string = "deleteWish"
const DELETE_WISH_TASK db.TaskType = "DELETE_WISH"

type TriggeredDeleteWishOperation struct {
	WishID  db.WishID
	Trigger db.Trigger
}

func TriggerDeleteWish(tx *sql.Tx, wishID db.WishID, reason string, details string) (db.Trigger, error) {
	trigger := db.Trigger{
		Action:   DELETE_WISH_ACTION,
		ObjectID: wishID,

		Timestamp: time.Now(),
		Reason:    reason,
		Details:   details,
	}

	trigger, err := db.CreateTrigger(tx, trigger)
	if err != nil {
		return db.Trigger{}, err
	}

	// Create and schedule the check
	op := TriggeredDeleteWishOperation{WishID: wishID, Trigger: trigger}
	operations.Schedule(&op, wishID.String(), DELETE_WISH_ACTION, time.Now())

	return trigger, nil
}

func DeleteWishTask(tx *sql.Tx, wishID db.WishID) (db.TaskPtr, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = db.GetTransaction()
		if err != nil {
			return db.TaskPtr{}, err
		}
		defer tx.Rollback()
		ownTx = true
	}

	foundTask := false
	titleStr := "Delete wish with ID " + wishID.String()
	// Get the wish
	wish, err := db.GetWishByID(tx, wishID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return db.TaskPtr{}, err
	} else if err == nil {
		foundTask = true
		titleStr = fmt.Sprintf("Delete wish %s -> %s, %s", wish.Identifier.InputString(), wish.Identifier.CvmfsRepository, wish.Identifier.Wishlist)
	}

	task, ptr, err := db.CreateTask(tx, DELETE_WISH_TASK, titleStr)
	if err != nil {
		return db.TaskPtr{}, err
	}

	if !foundTask {
		task.Log(tx, db.LOG_SEVERITY_INFO, "Wish not found, assuming it was already deleted")
		task.SetTaskCompleted(tx, db.TASK_RESULT_SUCCESS)
		return ptr, nil
	}

	if err := db.DeleteWishesByIDs(tx, []db.WishID{wishID}); err != nil {
		task.LogFatal(tx, fmt.Sprintf("Error deleting wish: %s", err))
		return ptr, nil
	}

	task.SetTaskCompleted(tx, db.TASK_RESULT_SUCCESS)

	if ownTx {
		if err := tx.Commit(); err != nil {
			task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Error committing transaction: %s", err))
			return ptr, nil
		}
	}

	return ptr, nil
}
