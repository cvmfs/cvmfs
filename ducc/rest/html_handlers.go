package rest

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/http"
	"sort"
	"text/template"
	"time"

	_ "embed"

	"github.com/cvmfs/ducc/daemon"
	"github.com/cvmfs/ducc/db"
	"github.com/google/uuid"
)

//go:embed templates/singleTaskPage.html
var taskHtmlTmpl string

//go:embed templates/imagesPage.html
var imagesHtmlTmpl string

//go:embed templates/frontPage.html
var frontPageHtmlTmpl string

//go:embed templates/operationsPage.html
var operationsHtmlTempl string

func frontPageHtmlHandler(w http.ResponseWriter, r *http.Request) {
	tx, err := db.GetTransaction()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	// Get all cvmfs repositories
	activeImages, err := db.GetImagesWithWish(tx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	repositories, err := db.GetAllCvmfsRepos(tx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	runningOperations, err := db.GetAllUnfinishedRootTasks(tx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pendingTriggers, err := db.GetPendingTriggers(tx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	activeImagesIDs := make([]uuid.UUID, len(activeImages))
	for i, image := range activeImages {
		activeImagesIDs[i] = image.ID
	}
	lastImageUpdate, err := db.GetLastTriggeredTaskByObjectIDs(tx, []string{daemon.UPDATE_IMAGE_ACTION}, activeImagesIDs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type imageWithUpdateStatus struct {
		ImageName         string
		ShortID           string
		ImageID           string
		LastTaskID        string
		InProgress        string
		Result            string
		LastTaskStarted   string
		LastTaskCompleted string
	}

	type templateData struct {
		NumActiveImages      int
		NumCvmfsRepos        int
		NumRunningOperations int
		NumPendingTriggers   int
		ImagesWithStatus     []imageWithUpdateStatus
	}

	data := templateData{
		NumActiveImages:      len(activeImages),
		NumCvmfsRepos:        len(repositories),
		NumRunningOperations: len(runningOperations),
		NumPendingTriggers:   len(pendingTriggers),
		ImagesWithStatus:     make([]imageWithUpdateStatus, 0),
	}

	for i, task := range lastImageUpdate {
		status := imageWithUpdateStatus{
			ImageName:  activeImages[i].GetSimpleName(),
			ImageID:    activeImages[i].ID.String(),
			ShortID:    activeImages[i].ID.String()[:8],
			LastTaskID: task.ID.String(),
			Result:     string(task.Result),
		}
		if task.Status == db.TASK_STATUS_PENDING || task.Status == db.TASK_STATUS_RUNNING {
			status.InProgress = string(task.Status)
		}
		if !task.StartTimestamp.IsZero() {
			status.LastTaskStarted = humanReadableDuration(time.Since(task.DoneTimestamp)) + " ago"
		}
		if !task.DoneTimestamp.IsZero() {
			status.LastTaskCompleted = humanReadableDuration(time.Since(task.StartTimestamp)) + " ago"
		}
		data.ImagesWithStatus = append(data.ImagesWithStatus, status)
	}

	t := template.Must(template.New("frontPageTemplate").Parse(frontPageHtmlTmpl))
	var buf bytes.Buffer
	err = t.Execute(&buf, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(buf.Bytes())
}

func operationsHtmlHandler(w http.ResponseWriter, r *http.Request) {
	// Check if the parameter "all" is set
	all := false
	if _, ok := r.URL.Query()["all"]; ok {
		all = true
	}

	rootTasks, err := db.GetRootTasks(nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var includedTasks []db.SmallTaskSnapshot
	if all {
		includedTasks = rootTasks
	} else {
		for _, task := range rootTasks {
			if task.Status != db.TASK_STATUS_DONE {
				includedTasks = append(includedTasks, task)
			}
		}
	}

	// Sort the tasks by their creation time
	sort.Slice(includedTasks, func(i, j int) bool {
		return includedTasks[i].CreatedTimestamp.After(includedTasks[j].CreatedTimestamp)
	})

	type TemplateData struct {
		Tasks       []db.SmallTaskSnapshot
		All         bool
		NumExcluded int
	}

	data := TemplateData{
		Tasks:       includedTasks,
		All:         all,
		NumExcluded: len(rootTasks) - len(includedTasks),
	}

	t := template.Must(template.New("operationsTemplate").Parse(operationsHtmlTempl))
	var buf bytes.Buffer
	err = t.Execute(&buf, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(buf.Bytes())
}

func getTaskHtmlHandler(w http.ResponseWriter, r *http.Request) {
	taskIdStr := getField(r, 0)
	id, err := uuid.Parse(taskIdStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = db.GetTaskSnapshotByID(nil, db.TaskID(id))
	if err == sql.ErrNoRows {
		http.Error(w, fmt.Sprintf("Task with id %s not found", taskIdStr), http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	html, err := renderTaskTemplate(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(html)
}

func getImagesHtmlHandler(w http.ResponseWriter, r *http.Request) {
	type TemplateData struct {
		Images []db.Image
	}

	images, err := db.GetAllImages(nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := TemplateData{
		Images: images,
	}

	t := template.Must(template.New("imagesTemplate").Parse(imagesHtmlTmpl))
	var buf bytes.Buffer
	err = t.Execute(&buf, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(buf.Bytes())
}

func renderTaskTemplate(taskId db.TaskID) ([]byte, error) {
	type TemplateData struct {
		TaskSnapshot   db.FullTaskSnapshot
		ChildSnapshots []db.FullTaskSnapshot
	}

	tx, err := db.GetTransaction()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Get the task snapshot
	snapshot, err := db.GetTaskSnapshotByID(tx, taskId)
	if err != nil {
		return nil, err
	}

	// Get the child snapshots
	childSnapshots, err := db.GetTaskSnapshotsByIDs(tx, snapshot.SubtaskIDs)
	if err != nil {
		return nil, err
	}

	logSeverityToText := func(severity db.LogSeverity) string {
		switch severity {
		case db.LOG_SEVERITY_DEBUG:
			return "debug"
		case db.LOG_SEVERITY_INFO:
			return "info"
		case db.LOG_SEVERITY_WARN:
			return "warning"
		case db.LOG_SEVERITY_ERROR:
			return "error"
		case db.LOG_SEVERITY_FATAL:
			return "fatal"
		default:
			return "unknown"
		}
	}

	data := TemplateData{
		TaskSnapshot:   snapshot,
		ChildSnapshots: childSnapshots,
	}

	t := template.Must(template.New("taskTemplate").Funcs(template.FuncMap{"LogSeverityToText": logSeverityToText}).Parse(taskHtmlTmpl))
	var buf bytes.Buffer
	err = t.Execute(&buf, data)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func humanReadableDuration(d time.Duration) string {
	hours := d / time.Hour
	d -= hours * time.Hour

	minutes := d / time.Minute
	d -= minutes * time.Minute

	seconds := d / time.Second

	if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}
