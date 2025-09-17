package pkg

import (
	"encoding/json"
	"fmt"
	"os"

	pathlib "github.com/chigopher/pathlib"
	"github.com/rs/zerolog/log"
)

type Changelog struct {
	FilesCreated       []string
	DirectoriesCreated []string
	LinksCreated       []string
	PathsDeleted       []string
	FilesPurged        []string
	RevisionNum        string
}

// Print out the info from the dryrun
func printDryrun(changelog Changelog) {
	fmt.Println("Files Created:")
	for _, file := range changelog.FilesCreated {
		fmt.Println(file)
	}
	fmt.Println("Directories Created:")
	for _, dir := range changelog.DirectoriesCreated {
		fmt.Println(dir)
	}
	fmt.Println("Links Created:")
	for _, link := range changelog.LinksCreated {
		fmt.Println(link)
	}
	fmt.Println("Paths Deleted:")
	for _, file := range changelog.PathsDeleted {
		fmt.Println(file)
	}
	fmt.Println("Files Purged:")
	for _, file := range changelog.FilesPurged {
		fmt.Println(file)
	}
}

// Get the data for the changelog from the db
func getChangelogData(database DB, revisionNum string) (Changelog, error) {
	var changelog Changelog
	var err error
	changelog.FilesCreated, err = database.QueryFiles()
	if err != nil {
		log.Error().Err(err).Msg("Failed to get db files created")
		return changelog, err
	}
	changelog.LinksCreated, err = database.QueryLinks()
	if err != nil {
		log.Error().Err(err).Msg("Failed to get db links created")
		return changelog, err
	}
	changelog.DirectoriesCreated, err = database.QueryDirs()
	if err != nil {
		log.Error().Err(err).Msg("Failed to get db dirs created")
		return changelog, err
	}
	FilesDeleted, DirectoriesDeleted, LinksDeleted, err := database.QueryDeletes()
	if err != nil {
		return changelog, err
	}
	changelog.PathsDeleted = append(append(FilesDeleted, DirectoriesDeleted...), LinksDeleted...)
	purgeFiles := database.QueryPurges()
	for _, purgeFile := range purgeFiles {
		changelog.FilesPurged = append(changelog.FilesPurged, purgeFile.PathStr)
	}
	changelog.RevisionNum = revisionNum
	return changelog, nil
}

// Print json changelog to provided changelog path
func printToChangelogFile(jsonChangelog []byte, changelogPathString string) (err error) {
	var changelogFile *os.File
	changelogFile, err = os.Create(changelogPathString)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create changelog")
		return err
	}
	defer func() {
		if tempErr := changelogFile.Close(); tempErr != nil {
			log.Error().Err(tempErr).Msg("Error in cleanup")
			if err == nil {
				err = tempErr
			}
		}
	}()
	if _, err = changelogFile.Write(jsonChangelog); err != nil {
		log.Error().Err(err).Msg("Error writing changelog")
		return err
	}
	return err
}

// Creates changelog and performs a dryrun if that functionality is requested
func CreateChangelog(dryrun bool, changelogPath *pathlib.Path, database DB, revisionNum string) error {
	changelog, err := getChangelogData(database, revisionNum)
	if err != nil {
		return err
	}
	var jsonChangelog []byte
	jsonChangelog, err = json.MarshalIndent(changelog, "", "\t")
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal changelog")
		return err
	}

	if changelogPath != nil {
		if err := printToChangelogFile(jsonChangelog, changelogPath.Clean().String()); err != nil {
			return err
		}
	} else {
		log.Info().Bytes("Json changelog", jsonChangelog).Msg("Changelog of rsync")
	}
	if dryrun {
		printDryrun(changelog)
	}
	return nil
}
