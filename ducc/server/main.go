package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/cvmfs/ducc/lib"
	cvmfs "github.com/cvmfs/ducc/server/cvmfs"
	res "github.com/cvmfs/ducc/server/replies"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/julienschmidt/httprouter"
	"github.com/syndtr/gocapability/capability"
)

type Repo struct {
	*cvmfs.Repository
}

func NewRepo(name string) Repo {
	r := cvmfs.NewRepository(name)
	return Repo{r}
}

func (repo *Repo) Exists() bool {
	path := repo.Root()
	stat, err := os.Stat(path)
	if err != nil {
		return false
	}
	return stat.IsDir()
}

func (repo *Repo) GetOwnerUID() int {
	path := repo.Root()

	stat, _ := os.Stat(path)
	if stat, ok := stat.Sys().(*syscall.Stat_t); ok {
		return int(stat.Uid)
	}
	return -1
}

func (repo *Repo) statusLayer(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	// the layer to be correctly ingested, need to:
	// *. Exists
	// *. Have the layerfs directory
	// *. Have the .cvmfs catalog inside the layerfs directory
	// if all these conditions are met, the layer is correctly ingested

	lib.Log().Info("[BEGIN] status layer: ", p.ByName("digest"))
	defer lib.Log().Info("[DONE]  status layer: ", p.ByName("digest"))

	layerErrors := make([]res.DUCCStatError, 0)
	pathsToCheck := make([]string, 0, 5)

	path := lib.LayerPath(repo.Name, p.ByName("digest"))
	pathsToCheck = append(pathsToCheck, path)

	layerfs := lib.LayerRootfsPath(repo.Name, p.ByName("digest"))
	catalog := filepath.Join(layerfs, ".cvmfscatalog")
	pathsToCheck = append(pathsToCheck, layerfs)
	pathsToCheck = append(pathsToCheck, catalog)

	for _, path := range pathsToCheck {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			layerErrors = append(layerErrors, res.NewDUCCStatError(path, err))
		}
	}

	if len(layerErrors) == 0 {
		result := res.NewLayerStatusOk()
		response, jsonErr := json.Marshal(result)
		if jsonErr != nil {
			http.Error(w, jsonErr.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		w.Write(response)
	} else {
		result := res.NewLayerStatusErr(layerErrors)
		response, jsonErr := json.Marshal(result)
		if jsonErr != nil {
			http.Error(w, jsonErr.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		w.Header().Set("Content-Type", "application/json")
		w.Write(response)
	}
}

// to ingest layers we need to create the directory structure
// add all the catalogs in there
// before to create anything we need to make sure that the stuff are not there
// we should try to limit the number of transactions
// one idea would be to first check what is missing, and the in one transaction batch all the changes

// add directory
// add file
// add link
// remove directory
// remove file
// remove link

func (repo Repo) ingestLayerFileSystem(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	log.Printf("Receiving new layer: %s", p.ByName("digest"))
	defer log.Printf("Done with layer: %s", p.ByName("digest"))
	// we get the path where to ingest the layer
	layerfs := lib.LayerRootfsPath(repo.Name, p.ByName("digest"))

	layerParent := lib.LayerParentPath(repo.Name, p.ByName("digest"))
	createCvmfsCatalog := cvmfs.NewAddCVMFSCatalog(layerParent)
	index, err := repo.AddFSOperations(createCvmfsCatalog)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = repo.WaitFor(index)
	if err != nil {
		if errors.Is(err, cvmfs.WaitForNotScheduledError) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	gzip, err := gzip.NewReader(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer gzip.Close()

	ingestTar := cvmfs.NewIngestTar(gzip, layerfs)
	index, err = repo.AddFSOperations(ingestTar)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = repo.WaitFor(index)
	if err != nil {
		if errors.Is(err, cvmfs.WaitForNotScheduledError) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if ingestTar.FirstError() != nil {
		fmt.Println(ingestTar.FirstError())
		http.Error(w, ingestTar.FirstError().Error(), http.StatusInternalServerError)
		return
	}
	result := res.NewLayerSuccessfullyIngested(p.ByName("digest"))
	response, jsonErr := json.Marshal(result)
	if jsonErr != nil {
		http.Error(w, jsonErr.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
	return
}

func (repo Repo) ingestLayerOrigin(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
}

func (repo Repo) checkLayerExists(digest string) error {
	layerfs := lib.LayerRootfsPath(repo.Name, digest)
	_, err := os.Stat(layerfs)
	return err
}

func (repo Repo) statusFlat(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")

	// we just check if there is a symlink
	// the symlink points to a directory
	// the directory is populated

	fullPath := filepath.Join(repo.Root(), name)

	// we check if there is a symlink
	_, err := os.Lstat(fullPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// we check if the link points to something
	stat, err := os.Stat(fullPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// we check we point to a directory
	if stat.Mode().IsDir() == false {
		err = fmt.Errorf("error symlink does not point to a directory, name: %s", fullPath)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	f, err := os.Open(fullPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	finfo, err := f.Readdir(5)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if len(finfo) <= 0 {
		err = fmt.Errorf("error symlink points to empty directory, name: %s", fullPath)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	dirName, err := os.Readlink(fullPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/text")
	w.Write([]byte(filepath.Base(dirName)))
	return
}

func (repo Repo) ingestFlat(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	lib.Log().Info("[BEGIN] ingesting flat image")
	defer lib.Log().Info("[DONE] ingesting flat")
	// we need to read the manifest
	manifest := schema2.DeserializedManifest{}
	manifestByte, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	err = manifest.UnmarshalJSON(manifestByte)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// we create the directory for the flat fs
	flatDigest := strings.Split(manifest.Config.Digest.String(), ":")[1]
	flatPath := filepath.Join(repo.Root(), ".flat", flatDigest[0:2], flatDigest)

	writeSuccessfulReply := func() {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(flatDigest))
		w.Header().Set("Content-Type", "application/text")
	}

	// we finish by creating the symlink
	name := ps.ByName("name")
	humanDir := filepath.Join(repo.Root(), name)

	fmt.Println("Checking on flatPath: ", flatPath)
	if flatInfo, err := os.Stat(flatPath); err == nil {
		name := ps.ByName("name")
		humanDir := filepath.Join(repo.Root(), name)

		fmt.Println("Checking on humanDir: ", humanDir)
		humanInfo, err := os.Stat(humanDir)
		if err == nil {
			if os.SameFile(flatInfo, humanInfo) {
				writeSuccessfulReply()
				return
			}
		}

		manifestPath := filepath.Join(repo.Root(), ".metadata", name, "manifest.json")
		addManifest := cvmfs.NewCreateRegularFile(manifestPath, manifestByte)
		link := cvmfs.NewCreateSymlink(humanDir, flatPath)

		repo.AddFSOperations(addManifest)
		repo.AddFSOperations(link)

		fmt.Println("shortcut just as name")
		if err := link.FirstError(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeSuccessfulReply()
		return
	}

	// we check that all layers are there
	for _, layer := range manifest.Layers {
		layerName := strings.Split(layer.Digest.String(), ":")[1]
		fmt.Println("checking layer: ", layerName)
		if err := repo.checkLayerExists(layerName); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		fmt.Println("layer exists: ", layerName)
	}

	// create the directory
	repo.AddFSOperations(cvmfs.NewCreateDirectory(flatPath))

	// we apply all the layers on top of the single target
	layers := []string{}
	for _, layer := range manifest.Layers {
		layerName := strings.Split(layer.Digest.String(), ":")[1]
		layers = append(layers, lib.LayerRootfsPath(repo.Name, layerName))
	}

	// we create the flat fs
	applyDirs := cvmfs.NewApplyDirectories(flatPath, layers...)
	repo.AddFSOperations(applyDirs)

	// fix the permissions of the directory to fit singularity
	repo.AddFSOperations(cvmfs.NewFixPerms(flatPath))
	repo.AddFSOperations(cvmfs.NewMakeSingularityEnv(flatPath))

	link := cvmfs.NewCreateSymlink(humanDir, flatPath)
	repo.AddFSOperations(link)

	manifestPath := filepath.Join(repo.Root(), ".metadata", name, "manifest.json")
	repo.AddFSOperations(cvmfs.NewCreateRegularFile(manifestPath, manifestByte))

	if err := applyDirs.FirstError(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := link.FirstError(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeSuccessfulReply()
	return
}

func main() {
	c, err := capability.NewPid2(0)
	if err != nil {
		lib.LogE(err).Fatal("Error in obtaining the necessary capability")
		return
	}
	if err := c.Load(); err != nil {
		lib.LogE(err).Fatal("Error in loading the capability for this process")
		return
	}
	neededCaps := []capability.Cap{capability.CAP_CHOWN, capability.CAP_DAC_OVERRIDE, capability.CAP_DAC_READ_SEARCH, capability.CAP_FOWNER, capability.CAP_FSETID, capability.CAP_SETUID, capability.CAP_SETGID}
	capNeeded := ""
	for _, capability := range neededCaps {
		capNeeded = fmt.Sprintf("%s %s", capNeeded, capability.String())
	}
	missingCapError := fmt.Errorf("Missing capability, we need: %s", capNeeded)
	for _, neededCap := range neededCaps {
		if c.Get(capability.EFFECTIVE, neededCap) == false {
			lib.LogE(missingCapError).Fatal("Detected missing effective capability: ", neededCap.String())
		}
	}

	if len(os.Args) < 2 {
		err := fmt.Errorf("Missing repository name, provide it as first argument.")
		lib.LogE(err).Fatal()
	}

	repositoryName := os.Args[1]
	repo := NewRepo(repositoryName)

	if repo.Exists() == false {
		lib.LogE(fmt.Errorf("The repository does not seems to exists: %s", repositoryName)).Fatal()
	}

	user, _ := user.Current()
	uidS := user.Uid
	uid, err := strconv.Atoi(uidS)
	if err != nil {
		lib.LogE(fmt.Errorf("Error in getting the current UID")).Fatal()
	}
	if uid != repo.GetOwnerUID() {
		lib.LogE(fmt.Errorf("Error, the user running is not the owner of the repository")).Fatal()
	}

	go repo.StartOperationsLoop()
	router := httprouter.New()
	router.GET("/layer/status/:digest", repo.statusLayer)
	router.POST("/layer/filesystem/:digest", repo.ingestLayerFileSystem)
	router.POST("/layer/origin/:digest", repo.ingestLayerOrigin)
	router.GET("/flat/human/*name", repo.statusFlat)
	router.POST("/flat/human/*name", repo.ingestFlat)
	router.GET("/image/metadata/manifest/*name", nil)
	router.GET("/image/metadata/config/*name", nil)

	log.Fatal(http.ListenAndServe(":8080", router))
}
