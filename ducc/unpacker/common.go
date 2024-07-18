package unpacker

import (
	"fmt"
	"time"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
)

// struct for entries in layers.json
type LayerInfo struct {
	ID                   string    `json:"id,omitempty"`
	Parent               string    `json:"parent,omitempty"`
	Created              time.Time `json:"created,omitempty"`
	CompressedDiffDigest string    `json:"compressed-diff-digest,omitempty"`
	CompressedSize       int       `json:"compressed-size,omitempty"`
	UncompressedDigest   string    `json:"diff-digest,omitempty"`
	UncompressedSize     int64     `json:"diff-size,omitempty"`
}

func UpdateImageInRepoTask(image db.Image, manifest registry.ManifestWithBytesAndDigest, outputOptions db.WishOutputOptions, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Update image %s to CVMFS repo %s", image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_UPDATE_IMAGE_IN_REPO, titleStr)
	if err != nil {
		return db.TaskPtr{}, err
	}

	// Sanity check options
	if outputOptions.CreatePodman.Value && !outputOptions.CreateLayers.Value {
		task.LogFatal(nil, "Cannot create podman output without creating layers")
		return ptr, err
	}
	if outputOptions.CreateThinImage.Value && !outputOptions.CreateLayers.Value {
		task.LogFatal(nil, "Cannot create thin image output without creating layers")
		return ptr, err
	}

	// Create the output tasks
	var (
		createFlatTask   db.TaskPtr
		createLayersTask db.TaskPtr
		createPodmanTask db.TaskPtr
		//createThinTask   db.TaskPtr
	)

	if outputOptions.CreateFlat.Value {
		createFlatTask, err = CreateFlat(image, manifest, cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create create flat task: %s", err.Error()))
			return ptr, err
		}
		if err := task.LinkSubtask(nil, createFlatTask); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to link create flat task: %s", err.Error()))
			return ptr, err
		}
	}

	if outputOptions.CreateLayers.Value {
		createLayersTask, err = CreateLayers(image, manifest, cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create create layers task: %s", err.Error()))
			return ptr, err
		}
		if err := task.LinkSubtask(nil, createLayersTask); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to link create layers task: %s", err.Error()))
			return ptr, err
		}
	}

	if outputOptions.CreatePodman.Value {
		createPodmanTask, err = CreatePodman(image, manifest, cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create create podman task: %s", err.Error()))
			return ptr, err
		}
		if err := task.LinkSubtask(nil, createPodmanTask); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to link create podman task: %s", err.Error()))
			return ptr, err
		}
	}

	/*if outputOptions.CreateThinImage.Value {
		createThinTask, err = CreateThinImage(image, manifest, cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create create thin image task: %s", err.Error()))
			return ptr, err
		}
		if err := task.LinkSubtask(nil, createThinTask); err != nil{
			task.LogFatal(nil, fmt.Sprintf("Failed to link create thin image task: %s", err.Error()))
			return ptr, err
		}
	}*/

	go func() {
		if !task.WaitForStart() {
			return
		}

		// Wa can start createFlatTask and createLayersTask in parallel
		if outputOptions.CreateLayers.Value {
			createLayersTask.Start(nil)
			task.Log(nil, db.LOG_SEVERITY_INFO, "Started creating layers")
		}
		if outputOptions.CreateFlat.Value {
			createFlatTask.Start(nil)
			task.Log(nil, db.LOG_SEVERITY_INFO, "Started creating flat image")
		}
		// Podman has to wait for createLayersTask to finish
		if outputOptions.CreatePodman.Value {
			go func() {
				createLayersResult := createLayersTask.WaitUntilDone()
				if !db.TaskResultSuccessful(createLayersResult) {
					task.Log(nil, db.LOG_SEVERITY_ERROR, "Cancelling 'Create Podman' due to failed 'Create Layers' task")
					createPodmanTask.Cancel(nil)
					return
				}
				createPodmanTask.Start(nil)
				task.Log(nil, db.LOG_SEVERITY_INFO, "Started updating Podman store")
			}()
		}

		someTaskFailed := false
		// Wait for all tasks to finish
		if outputOptions.CreateLayers.Value {
			result := createLayersTask.WaitUntilDone()
			if db.TaskResultSuccessful(result) {
				task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully created layers")
			} else {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Failed to create layers")
				someTaskFailed = true
			}
		}
		if outputOptions.CreateFlat.Value {
			result := createFlatTask.WaitUntilDone()
			if db.TaskResultSuccessful(result) {
				task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully created flat image")
			} else {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Failed to create flat image")
				someTaskFailed = true
			}
		}
		if outputOptions.CreatePodman.Value {
			result := createPodmanTask.WaitUntilDone()
			if db.TaskResultSuccessful(result) {
				task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully updated Podman store")
			} else {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Failed to update Podman store")
				someTaskFailed = true
			}
		}

		if someTaskFailed {
			task.Log(nil, db.LOG_SEVERITY_ERROR, "Not all tasks were successful")
			task.SetTaskCompleted(nil, db.TASK_RESULT_FAILURE)
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "All tasks completed successfully")
		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
	}()

	return ptr, nil
}
