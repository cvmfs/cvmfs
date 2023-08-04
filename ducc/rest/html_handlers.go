package rest

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/http"
	"text/template"

	_ "embed"

	"github.com/cvmfs/ducc/db"
	"github.com/google/uuid"
)

//go:embed templates/singleTaskPage.html
var taskHtmlTmpl string

//go:embed templates/imagesPage.html
var imagesHtmlTmpl string

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
		TaskSnapshot   db.TaskSnapshot
		ChildSnapshots []db.TaskSnapshot
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
