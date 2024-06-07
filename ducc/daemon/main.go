package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/cvmfs/ducc/daemon/scheduler"
	"github.com/cvmfs/ducc/db"
	"github.com/google/uuid"
)

var operations = scheduler.NewScheduler[interface{}](1)
var initOk bool = false

func Init(schedulingEnabled bool) error {
	if err := acquireDaemonLock(); err != nil {
		return fmt.Errorf("error acquiring daemon lock: %w", err)
	}

	tx, err := db.GetTransaction()
	if err != nil {
		return fmt.Errorf("could not init daemon, error getting transaction: %w", err)
	}
	defer tx.Rollback()

	// TODO: Print some stats if there was an improper shutdown

	// We first need to clean up unfinished tasks and restore pending triggers
	if err := restoreUnfinishedTriggers(tx); err != nil {
		return fmt.Errorf("error restoring pending triggers: %w", err)
	}
	if err := abortUnfinishedTasks(tx); err != nil {
		return fmt.Errorf("error aborting unfinished tasks: %w", err)
	}

	if schedulingEnabled {
		// Schedule expand wildcards
		wishes, err := db.GetAllWishes(tx)
		if err != nil {
			return fmt.Errorf("error getting all wishes: %w", err)
		}
		for _, wish := range wishes {
			if wish.Identifier.InputTagWildcard {
				scheduleUpdateWish(tx, wish)
			}
		}

		// Schedule image updates
		images, err := db.GetAllImages(tx)
		if err != nil {
			return fmt.Errorf("error getting all images: %w", err)
		}
		for _, image := range images {
			scheduleUpdateImage(tx, image)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}
	initOk = true
	return nil
}

func Run(ctx context.Context, done chan<- any) error {
	if !initOk {
		return fmt.Errorf("daemon.Run() called without calling daemon.Init() first\n")
	}
	defer close(done)
	for {
		toCombine := make([]interface{}, 1)
		var objectID scheduler.ObjectIDType
		var actionID scheduler.ActionIDType
		toCombine[0], objectID, actionID, _ = operations.WaitForNextOperation(ctx)
		if toCombine[0] == nil || reflect.ValueOf(toCombine[0]).IsNil() {
			// We got a timeout
			return nil
		}
		// Check if there are more operations for this object and action
		otherOperations := operations.GetOperationsForObjectAction(objectID, actionID)
		for _, otherOperation := range otherOperations {
			if otherOperation.NextRun.Before(time.Now()) {
				// This operation is due, so we can combine it with the one we just got
				// and process them together
				toCombine = append(toCombine, otherOperation.Obj)
				operations.RemoveOperation(otherOperation.ObjectID, otherOperation.ActionID, otherOperation.OperationID)
				continue
			}
		}

		tx, err := db.GetTransaction()
		if err != nil {
			panic(fmt.Errorf("error getting transaction: %s", err))
		}
		defer tx.Rollback()

		// Get or create triggers
		triggerIDs := make([]uuid.UUID, len(toCombine))
		var taskPtr db.TaskPtr

		switch actionID {
		case UPDATE_IMAGE_ACTION:
			imageID := uuid.MustParse(objectID)
			image, err := db.GetImageByID(tx, imageID)
			if err != nil {
				// TODO: Handle
				fmt.Printf("Could not get image. Is it deleted?: %s\n", err)
				continue
			}

			for i, operation := range toCombine {
				switch operation := reflect.ValueOf(operation).Elem().Interface().(type) {
				case *TriggeredUpdateImageOperation:
					// If this is a triggered operation, we already have a trigger
					triggeredImageUpdate := operation
					triggerIDs[i] = triggeredImageUpdate.trigger.ID
				case *ScheduledUpdateImageOperation:
					// If this is a scheduled operation, we need to create a trigger
					trigger := db.Trigger{
						Action:   UPDATE_IMAGE_ACTION,
						ObjectID: image.ID,

						Timestamp: time.Now(),
						Reason:    "Scheduled update",
					}
					var err error
					trigger, err = db.CreateTrigger(tx, trigger)
					if err != nil {
						panic(fmt.Errorf("error creating trigger in DB: %s", err))
					}
					triggerIDs[i] = trigger.ID
				default:
					panic(fmt.Errorf("unknown operation object type: %T", operation))
				}
			}
			// Create task
			taskPtr, err = UpdateImageTask(tx, image)
			if err != nil {
				panic(fmt.Errorf("error creating task in DB: %s", err))
			}

		case UPDATE_WISH_ACTION:
			wishID := uuid.MustParse(objectID)
			wish, err := db.GetWishByID(tx, wishID)
			if err != nil {
				// TODO: Handle
				fmt.Printf("Could not get wish. Is it deleted?: %s\n", err)
			}
			forceUpdateImages := false

			for i, operation := range toCombine {
				switch operation := reflect.ValueOf(operation).Elem().Interface().(type) {
				case *TriggeredUpdateWishOperation:
					// If this is a triggered operation, we already have a trigger
					triggeredExpandWildcard := operation
					triggerIDs[i] = triggeredExpandWildcard.trigger.ID
					forceUpdateImages = operation.forceUpdateImages
				case *ScheduledUpdateWishOperation:
					// If this is a scheduled operation, we need to create a trigger
					trigger := db.Trigger{
						Action:   UPDATE_WISH_ACTION,
						ObjectID: wish.ID,

						Timestamp: time.Now(),
						Reason:    "Scheduled expand wildcard",
					}
					var err error
					trigger, err = db.CreateTrigger(tx, trigger)
					if err != nil {
						panic(fmt.Errorf("error creating trigger in DB: %s", err))
					}
					triggerIDs[i] = trigger.ID
				case *any:
					panic("Got *any")
				default:
					panic(fmt.Errorf("unknown operation object type: %T", operation))
				}
			}
			// Create task
			taskPtr, err = UpdateWishTask(tx, wish, forceUpdateImages)
			if err != nil {
				panic(fmt.Errorf("error creating task in DB: %s", err))
			}
		case DELETE_WISH_ACTION:
			// We have pre-created triggers for this action, so we just need to get the IDs
			for i, operation := range toCombine {
				trigger := reflect.ValueOf(operation).Elem().Interface().(*TriggeredDeleteWishOperation).Trigger
				triggerIDs[i] = trigger.ID
			}
			// There is only one operation for this action, so we take the first one
			operation := reflect.ValueOf(toCombine[0]).Elem().Interface().(*TriggeredDeleteWishOperation)
			// Delete the wish
			taskPtr, err = DeleteWishTask(tx, operation.WishID)
			if err != nil {
				panic(fmt.Errorf("error creating task in DB: %s", err))
			}
			// Remove the object from the operations map, no need to trigger it again
			operations.RemoveObject(operation.WishID.String())
		default:
			panic(fmt.Errorf("unknown action: %s", actionID))
		}
		// Link the triggers to the task
		if err := db.LinkTriggersToTask(tx, taskPtr.GetValue().ID, triggerIDs); err != nil {
			panic(fmt.Errorf("error linking triggers to task: %s", err))
		}
		if err := tx.Commit(); err != nil {
			panic(fmt.Errorf("error committing transaction: %s", err))
		}

		// Set the object as busy, and start the task
		operations.IncrementObjectUseCount(objectID)
		// TODO: add override to execute serially
		go func() {
			taskPtr.Start(nil)
			taskPtr.WaitUntilDone()
			operations.DecrementObjectUseCount(objectID)
		}()
	}
}

func abortUnfinishedTasks(tx *sql.Tx) error {
	// Set all unfinished tasks to aborted
	unfinishedTasks, err := db.GetAllUnfinishedTasks(tx)
	if err != nil {
		return err
	}
	for _, task := range unfinishedTasks {
		if err := db.SetTaskStatusById(tx, task.ID, db.TASK_STATUS_DONE); err != nil {
			return err
		}
		if err := db.SetTaskResultById(tx, task.ID, db.TASK_RESULT_ABORTED); err != nil {
			return err
		}
		if err := db.AppendLogToTaskByID(tx, task.ID, db.LOG_SEVERITY_ERROR, "Aborted due to daemon restart"); err != nil {
			return err
		}

	}
	return nil
}

func restoreUnfinishedTriggers(tx *sql.Tx) error {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = db.GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		ownTx = true
	}

	triggers, err := db.GetUnfinishedTriggers(tx)
	if err != nil {
		return err
	}

	triggerIDs := make([]uuid.UUID, len(triggers))
	for i, trigger := range triggers {
		triggerIDs[i] = trigger.ID
	}
	if err := db.UnlinkTriggersFromTask(tx, triggerIDs); err != nil {
		return err
	}

	for _, trigger := range triggers {
		switch trigger.Action {
		case UPDATE_IMAGE_ACTION:
			image, err := db.GetImageByID(tx, trigger.ObjectID)
			if err != nil {
				return err
			}
			op := &TriggeredUpdateImageOperation{
				image:   image,
				trigger: trigger,
			}
			operations.Schedule(op, image.ID.String(), UPDATE_IMAGE_ACTION, trigger.Timestamp)
		case UPDATE_WISH_ACTION:
			wish, err := db.GetWishByID(tx, trigger.ObjectID)
			if err != nil {
				return err
			}
			op := TriggeredUpdateWishOperation{
				wish:              wish,
				trigger:           trigger,
				forceUpdateImages: false, // TODO: Restore forceUpdateImages state. For now, we always force update
			}
			operations.Schedule(&op, wish.ID.String(), UPDATE_WISH_ACTION, trigger.Timestamp)
		case DELETE_WISH_ACTION:
			wishID := uuid.MustParse(trigger.ObjectID.String())
			op := TriggeredDeleteWishOperation{
				WishID:  wishID,
				Trigger: trigger,
			}
			operations.Schedule(&op, wishID.String(), DELETE_WISH_ACTION, trigger.Timestamp)
		default:
			return fmt.Errorf("unknown trigger action: %s", trigger.Action)
		}
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
