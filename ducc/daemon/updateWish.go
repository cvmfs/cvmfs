package daemon

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
)

const UPDATE_WISH_ACTION string = "updateWish"

type ScheduledUpdateWishOperation struct {
	wish db.Wish
}

type TriggeredUpdateWishOperation struct {
	wish              db.Wish
	trigger           db.Trigger
	forceUpdateImages bool
}

func UpdateWishTask(tx *sql.Tx, wish db.Wish, forceUpdateImages bool) (db.TaskPtr, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = db.GetTransaction()
		if err != nil {
			return db.NullTaskPtr(), err
		}
		defer tx.Rollback()
		ownTx = true
	}
	wildcardStr := fmt.Sprintf("%s/%s:%s", wish.Identifier.InputRegistryHostname, wish.Identifier.InputRepository, wish.Identifier.InputTag)
	task, ptr, err := db.CreateTask(tx, db.TASK_UPDATE_WISH, "Update wish "+wildcardStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return db.NullTaskPtr(), err
		}
	}

	go func() {
		task.WaitForStart()
		new, updated, deleted, err := registry.ExpandWildcardAndStoreImages(wish)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Error expanding wildcard: %s", err))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, fmt.Sprintf("%d new image(s), %d existing image(s) linked to this wish, %d image(s) unlinked from this wish", len(new), len(updated), len(deleted)))

		if forceUpdateImages {
			images, err := db.GetImagesByWishID(nil, wish.ID)
			if err != nil {
				task.LogFatal(nil, fmt.Sprintf("Error getting images for wish: %s", err))
				return
			}
			task.Log(nil, db.LOG_SEVERITY_INFO, fmt.Sprintf("Force updating all %d image(s) referenced by the wish", len(images)))
			for _, image := range images {
				TriggerUpdateImage(nil, image, "Wish update", "Wish updated with force update images enabled")
			}
		} else {
			// Start checks for the new images
			for _, image := range new {
				// TODO: Start checks
				TriggerUpdateImage(nil, image, "New image", fmt.Sprintf("New image %s, added because of wildcard expression %s", image.ID.String(), wish.Identifier.InputString()))
				task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Started check for new image %s", image.ID.String()))
			}
			// Schedule checks for the updated images, in case this whish has a different update interval than previously
			for _, image := range updated {
				if err := scheduleUpdateImage(nil, image); err != nil {
					fmt.Fprintf(os.Stderr, "Error in scheduling check for image: %s\n", err)
					os.Exit(1)
				}
			}
			// Cancel checks for the deleted images
			for _, image := range deleted {
				operations.RemoveObject(image.ID.String())
			}
		}

		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully updated wish")
	}()

	return ptr, nil
}

func TriggerUpdateWish(tx *sql.Tx, wish db.Wish, forceUpdateImages bool, reason string, details string) (db.Trigger, error) {
	trigger := db.Trigger{
		Action:   UPDATE_WISH_ACTION,
		ObjectID: wish.ID,

		Timestamp: time.Now(),
		Reason:    reason,
		Details:   details,
	}

	trigger, err := db.CreateTrigger(tx, trigger)
	if err != nil {
		return db.Trigger{}, err
	}

	// Create and schedule the check
	triggeredUpdate := TriggeredUpdateWishOperation{wish: wish, trigger: trigger, forceUpdateImages: forceUpdateImages}
	operations.Schedule(&triggeredUpdate, wish.ID.String(), UPDATE_WISH_ACTION, time.Now())

	return trigger, nil
}

func scheduleUpdateWish(tx *sql.Tx, wish db.Wish) error {
	// Find out when the last task was done
	tasks, err := db.GetTasksForObjectID(tx, wish.ID, []string{UPDATE_WISH_ACTION}, []db.TaskStatus{db.TASK_STATUS_DONE}, 1)
	if err != nil {
		return err
	}
	var lastTask *db.SmallTaskSnapshot
	if len(tasks) > 0 {
		lastTask = &tasks[0]
	}

	var nextCheckTime time.Time
	if lastTask == nil {
		// No previous check, schedule one now
		nextCheckTime = time.Now()
	} else {
		// Calculate the next check time
		nextCheckTime = lastTask.DoneTimestamp.Add(time.Duration(wish.ScheduleOptions.UpdateInterval.Value) * time.Second)
		if nextCheckTime.Before(time.Now()) {
			// The next check time is in the past, schedule one now
			nextCheckTime = time.Now()
		}
	}

	// Create and schedule the check
	scheduledExpand := ScheduledUpdateWishOperation{wish: wish}
	operations.Schedule(&scheduledExpand, wish.ID.String(), UPDATE_WISH_ACTION, nextCheckTime)

	return nil
}
