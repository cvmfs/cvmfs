package rest

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/http"
	"sort"
	"text/template"

	_ "embed"

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
	imagesByRepo, err := db.GetImagesByCvmfsRepos(tx, repositories)
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

	if err := tx.Commit(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type cvmfsRepoWithImages struct {
		Repo   string
		Images []db.Image
	}

	type templateData struct {
		NumActiveImages      int
		NumCvmfsRepos        int
		NumRunningOperations int
		NumPendingTriggers   int
		CvmfsReposWithImages []cvmfsRepoWithImages
	}

	data := templateData{
		NumActiveImages:      len(activeImages),
		NumCvmfsRepos:        len(repositories),
		NumRunningOperations: len(runningOperations),
		NumPendingTriggers:   len(pendingTriggers),
		CvmfsReposWithImages: make([]cvmfsRepoWithImages, len(repositories)),
	}
	for i, repo := range repositories {
		data.CvmfsReposWithImages[i] = cvmfsRepoWithImages{
			Repo:   repo,
			Images: imagesByRepo[i],
		}
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
		return includedTasks[i].CreatedTimestamp.Before(includedTasks[j].CreatedTimestamp)
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
