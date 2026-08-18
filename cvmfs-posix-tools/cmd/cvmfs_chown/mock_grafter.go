package main

import (
	"fmt"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

type ExpectedPathOwnerGroup struct {
	dir               bool
	file              bool
	sym               bool
	path              string
	owner             int
	group             int
	noDotSchemeFile   bool
	noDotSchemeAbsent bool
}

func Mock_check_expected_graft_getter(ctx Context, expecPOG []ExpectedPathOwnerGroup) func(db pkg.DB, repo string, priority string, debug bool) (pkg.GraftMetrics, error) {
	absPath, err := pkg.GetAbsolutePath(pathlib.NewPath(pkg.CurrentDirectory))
	if err != nil {
		panic(err)
	}
	return func(db pkg.DB, repo string, priority string, debug bool) (pkg.GraftMetrics, error) {
		return Mock_check_expected_graft(ctx, db, repo, debug, absPath.Parent().Parent().Clean().String(), expecPOG)
	}
}

func Mock_check_expected_graft(ctx Context, db pkg.DB, repo string, debug bool, baseDir string, expecPOG []ExpectedPathOwnerGroup) (pkg.GraftMetrics, error) {
	log.Debug().Msg("Checking expected owner and group")
	files, err := db.QueryFilesFullData()
	if err != nil {
		panic(err)
	}
	fileMap := make(map[string]pkg.DBFile)
	for _, file := range files {
		fileMap[file.GetName()] = file
	}
	dirs, err := db.QueryDirsFullData()
	if err != nil {
		panic(err)
	}
	dirMap := make(map[string]pkg.DBDir)
	for _, dir := range dirs {
		dirMap[dir.GetName()] = dir
	}
	links, err := db.QueryLinksFullData()
	if err != nil {
		panic(err)
	}
	linkMap := make(map[string]pkg.DBLink)
	for _, link := range links {
		linkMap[link.GetName()] = link
	}
	for _, pog := range expecPOG {
		if !ctx.cfg.Repo.DotScheme && pog.noDotSchemeAbsent {
			continue
		}
		if !ctx.cfg.Repo.DotScheme && pog.noDotSchemeFile {
			pog.dir = false
			pog.sym = false
			pog.file = true
		}
		switch {
		case pog.dir:
			dActual, ok := dirMap[pog.path]
			if !ok {
				return pkg.GraftMetrics{}, fmt.Errorf("missing path: " + pog.path)
			}
			if dActual.GetGroup() != pog.group || dActual.GetOwner() != pog.owner {
				return pkg.GraftMetrics{}, fmt.Errorf("Path: " + dActual.GetName() + " group or owner is not correct. Expected: " + fmt.Sprint(pog.owner) + ", " + fmt.Sprint(pog.group) + " Actual: " + fmt.Sprint(dActual.GetOwner()) + ", " + fmt.Sprint(dActual.GetGroup()))
			}
		case pog.file:
			fActual, ok := fileMap[pog.path]
			if !ok {
				return pkg.GraftMetrics{}, fmt.Errorf("missing path: " + pog.path)
			}
			if fActual.GetGroup() != pog.group || fActual.GetOwner() != pog.owner {
				return pkg.GraftMetrics{}, fmt.Errorf("Path: " + fActual.GetName() + " group or owner is not correct. Expected: " + fmt.Sprint(pog.owner) + ", " + fmt.Sprint(pog.group) + " Actual: " + fmt.Sprint(fActual.GetOwner()) + ", " + fmt.Sprint(fActual.GetGroup()))
			}
		case pog.sym:
			lActual, ok := linkMap[pog.path]
			if !ok {
				return pkg.GraftMetrics{}, fmt.Errorf("missing path: " + pog.path)
			}
			if lActual.GetGroup() != pog.group || lActual.GetOwner() != pog.owner {
				return pkg.GraftMetrics{}, fmt.Errorf("Path: " + lActual.GetName() + " group or owner is not correct. Expected: " + fmt.Sprint(pog.owner) + ", " + fmt.Sprint(pog.group) + " Actual: " + fmt.Sprint(lActual.GetOwner()) + ", " + fmt.Sprint(lActual.GetGroup()))
			}
		default:
			return pkg.GraftMetrics{}, fmt.Errorf("impossible case")
		}
	}
	return pkg.GraftMetrics{}, nil
}
