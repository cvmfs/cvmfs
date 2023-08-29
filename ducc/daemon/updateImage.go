package daemon

import (
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/products"
	"github.com/cvmfs/ducc/registry"
)

const UPDATE_IMAGE_ACTION string = "updateImage"

type ScheduledUpdateImageOperation struct {
	image db.Image
}

type TriggeredUpdateImageOperation struct {
	image   db.Image
	trigger db.Trigger
}

func UpdateImageTask(tx *sql.Tx, image db.Image) (db.TaskPtr, error) {
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

	titleStr := fmt.Sprintf("Update image %s", image.GetSimpleName())
	task, ptr, err := db.CreateTask(tx, db.TASK_UPDATE_IMAGE, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	// Find what wishes reference this image
	wishes, err := db.GetWishesByImageID(tx, image.ID)
	if err != nil {
		task.LogFatal(tx, fmt.Sprintf("Error fetching wishes referencing image %s: %s", image.ID.String(), err))
		return ptr, nil
	}
	if len(wishes) == 0 {
		// No wishes reference this image, so we don't want to check it
		task.SetTaskCompleted(tx, db.TASK_RESULT_SUCCESS)
		task.Log(tx, db.LOG_SEVERITY_INFO, fmt.Sprintf("No wishes reference image %s, not checking", image.ID.String()))
		return ptr, nil
	}

	outputsByCvmfsRepo := make(map[string]db.WishOutputOptions)
	for _, wish := range wishes {
		// Check if wish is in cvmfs repo map
		outputs, ok := outputsByCvmfsRepo[wish.Identifier.CvmfsRepository]
		if !ok {
			outputsByCvmfsRepo[wish.Identifier.CvmfsRepository] = wish.OutputOptions
		} else {
			// Merge outputs so that all required products are created
			outputs.CreateLayers.Value = outputs.CreateLayers.Value || wish.OutputOptions.CreateLayers.Value
			outputs.CreateFlat.Value = outputs.CreateFlat.Value || wish.OutputOptions.CreateFlat.Value
			outputs.CreatePodman.Value = outputs.CreatePodman.Value || wish.OutputOptions.CreatePodman.Value
			outputs.CreateThinImage.Value = outputs.CreateThinImage.Value || wish.OutputOptions.CreateThinImage.Value
			outputsByCvmfsRepo[wish.Identifier.CvmfsRepository] = outputs
		}
	}

	fetchManifestTask, err := registry.FetchManifestTask(tx, image)
	if err != nil {
		task.LogFatal(tx, fmt.Sprintf("Error fetching manifest for image %s: %s", image.ID.String(), err))
		return ptr, nil
	}
	if err := task.LinkSubtask(tx, fetchManifestTask); err != nil {
		task.LogFatal(tx, fmt.Sprintf("Error linking fetch manifest task for image %s: %s", image.ID.String(), err))
		return ptr, nil
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return db.NullTaskPtr(), err
		}
	}

	go func() {
		if task.WaitForStart() == db.TASK_STATUS_DONE {
			return
		}

		fetchManifestTask.Start(nil)
		fetchManifestResult := fetchManifestTask.WaitUntilDone()
		if fetchManifestResult != db.TASK_RESULT_SUCCESS {
			task.LogFatal(nil, fmt.Sprintf("Error fetching manifest for image %s: %s", image.ID.String(), fetchManifestResult))
			return
		}
		artifact, err := fetchManifestTask.GetArtifact()
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Error fetching manifest for image %s: %s", image.ID.String(), err))
			return
		}
		manifest := artifact.(registry.ManifestWithBytesAndDigest)

		subTasks := make([]db.TaskPtr, 0, len(outputsByCvmfsRepo))
		for cvmfsRepo, outputs := range outputsByCvmfsRepo {
			subTask, err := products.UpdateImageInRepoTask(image, manifest, outputs, cvmfsRepo)
			if err != nil {
				task.LogFatal(nil, fmt.Sprintf("Error creating subtask for cvmfs repo %s: %s", cvmfsRepo, err))
				return
			}
			subTasks = append(subTasks, subTask)
			if err := task.LinkSubtask(nil, subTask); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Error linking subtask for cvmfs repo %s: %s", cvmfsRepo, err))
				return
			}
		}

		wg := sync.WaitGroup{}
		failed := atomic.Bool{}
		// Start update tasks for the wishes
		for _, wishTask := range subTasks {
			wg.Add(1)
			wishTask.Start(nil)
			go func(wishTask db.TaskPtr) {
				result := wishTask.WaitUntilDone()
				if result != db.TASK_RESULT_SUCCESS {
					task.LogFatal(nil, fmt.Sprintf("Error updating image: %s", result))
					failed.Store(true)
				}
				wg.Done()
			}(wishTask)
		}
		wg.Wait()

		if failed.Load() {
			task.SetTaskCompleted(nil, db.TASK_RESULT_FAILURE)
		} else {
			task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		}
	}()

	return ptr, nil
}

func TriggerUpdateImage(tx *sql.Tx, image db.Image, reason string, details string) (db.Trigger, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = db.GetTransaction()
		if err != nil {
			return db.Trigger{}, err
		}
		defer tx.Rollback()
		ownTx = true
	}

	// Create the trigger
	trigger := db.Trigger{
		Action:   UPDATE_IMAGE_ACTION,
		ObjectID: image.ID,

		Timestamp: time.Now(),
		Reason:    reason,
		Details:   details,
	}
	var err error
	trigger, err = db.CreateTrigger(tx, trigger)
	if err != nil {
		return db.Trigger{}, fmt.Errorf("error creating trigger in DB: %s", err)
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return db.Trigger{}, err
		}
	}

	// Send to scheduler
	triggeredUpdate := TriggeredUpdateImageOperation{
		image:   image,
		trigger: trigger,
	}
	operations.Schedule(&triggeredUpdate, image.ID.String(), UPDATE_IMAGE_ACTION, time.Now())

	return trigger, nil
}

func scheduleUpdateImage(tx *sql.Tx, image db.Image) error {
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

	// Find all whishes that match this image
	wishes, err := db.GetWishesByImageID(tx, image.ID)
	if err != nil {
		return err
	}

	if len(wishes) == 0 {
		// No wishes reference this image, so we don't want to check it
		return nil
	}

	// Out of all the wishes, find the one with the lowest update interval
	var shortestIntervalWish db.Wish
	for i, wish := range wishes {
		if i == 0 {
			shortestIntervalWish = wish
		} else if wish.ScheduleOptions.UpdateInterval.Value < shortestIntervalWish.ScheduleOptions.UpdateInterval.Value {
			shortestIntervalWish = wish
		}
	}

	// Find out when the last update for this image was
	tasks, err := db.GetTasksForObjectID(tx, image.ID, []string{UPDATE_IMAGE_ACTION}, []db.TaskStatus{db.TASK_STATUS_DONE}, 1)
	if err != nil {
		return err
	}

	// Done with the DB, commit the transaction
	if ownTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	var lastUpdate *db.SmallTaskSnapshot
	if len(tasks) > 0 {
		lastUpdate = &tasks[0]
	}

	// Calculate the next update time
	nextUpdateTime := lastUpdate.DoneTimestamp.Add(time.Duration(shortestIntervalWish.ScheduleOptions.UpdateInterval.Value) * time.Second)
	if nextUpdateTime.Before(time.Now()) {
		nextUpdateTime = time.Now()
	}

	// Create and schedule the update
	scheduledUpdate := ScheduledUpdateImageOperation{image: image}
	operations.Schedule(&scheduledUpdate, image.ID.String(), UPDATE_IMAGE_ACTION, nextUpdateTime)

	return nil
}
