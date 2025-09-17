package pkg

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

type DBCursor struct {
	inner    *sql.Rows
	scanArgs []any
	err      error
}

func (rows *DBCursor) Err() error {
	return rows.err
}

func (rows *DBCursor) Next() bool {
	hasNext := rows.inner.Next()
	if !hasNext {
		rows.err = rows.inner.Err()
		return false
	}
	rows.err = rows.inner.Scan(rows.scanArgs...)
	if rows.err != nil {
		rows.inner.Close()
		return false
	}
	return true
}

func NewDBCursor(rows *sql.Rows, dest ...any) *DBCursor {
	return &DBCursor{
		inner:    rows,
		scanArgs: dest,
		err:      nil,
	}
}

type DBFile struct {
	name     string
	mode     int
	mtime    int64
	owner    int
	grp      int
	size     int64
	hashes   string
	internal int
}

func (file *DBFile) DBInsert(db *sql.DB) error {
	sql := `INSERT INTO files (name, mode, mtime, owner, grp, size, hashes, internal, compressed)
	        VALUES (?,?,?,?,?,?,?,?,?)`
	if _, err := db.Exec(sql, file.name, file.mode, file.mtime, file.owner, file.grp, file.size, file.hashes, file.internal, FilesUncompressed); err != nil {
		return err
	}
	return nil
}

func (file *DBFile) DBQuery(db *sql.DB) (*DBCursor, error) {
	rows, err := db.Query(
		"SELECT name, mode, mtime, owner, grp, size, hashes, internal FROM files")
	if err != nil {
		return nil, err
	}
	return NewDBCursor(rows,
		&file.name, &file.mode, &file.mtime, &file.owner, &file.grp, &file.size, &file.hashes, &file.internal), nil
}

func (file *DBFile) DBCount(db *sql.DB) (int, error) {
	count := 0
	rows, err := db.Query("SELECT count(*) FROM files")
	if err != nil {
		return count, err
	}
	rows.Next()
	if err := rows.Scan(&count); err != nil {
		log.Error().Err(err).Msg("Failed to get file count")
		return count, err
	}
	log.Debug().Int("Files Grafted", count).Msg("File Count")
	return count, nil
}

type DBFileReplace struct {
	name            string
	mode            int
	mtime           int64
	owner           int
	grp             int
	size            int64
	hashes          string
	checksum        string
	srcInfo         fs.FileInfo
	alternateBucket bool
}

type DBLink struct {
	name   string
	target string
	mtime  int64
	owner  int
	grp    int
	skip   int
}

func (link *DBLink) DBReplace(db *sql.DB) error {
	sql := `REPLACE INTO links (name, target, mtime, owner, grp, skip_if_file_or_dir)
	        VALUES (?,?,?,?,?,?)`
	if _, err := db.Exec(sql, link.name, link.target, link.mtime, link.owner, link.grp, link.skip); err != nil {
		return err
	}
	return nil
}

func (link *DBLink) DBInsert(db *sql.DB) error {
	sql := `INSERT INTO links (name, target, mtime, owner, grp, skip_if_file_or_dir)
	        VALUES (?,?,?,?,?,?)`
	if _, err := db.Exec(sql, link.name, link.target, link.mtime, link.owner, link.grp, link.skip); err != nil {
		return err
	}
	return nil
}

func (link *DBLink) DBQuery(db *sql.DB) (*DBCursor, error) {
	rows, err := db.Query(
		"SELECT name, target, mtime, owner, grp, skip_if_file_or_dir FROM links")
	if err != nil {
		return nil, err
	}
	return NewDBCursor(
		rows, &link.name, &link.target, &link.mtime, &link.owner, &link.grp, &link.skip), nil
}

func (link *DBLink) DBCount(db *sql.DB) (int, error) {
	count := 0
	rows, err := db.Query("SELECT count(*) FROM links")
	if err != nil {
		return count, err
	}
	rows.Next()
	if err := rows.Scan(&count); err != nil {
		log.Error().Err(err).Msg("Failed to get file count")
		return count, err
	}
	log.Debug().Int("Files Grafted", count).Msg("File Count")
	return count, nil
}

type DBLinkReplace struct {
	name   string
	target string
	mtime  int64
	owner  int
	grp    int
	skip   int
}

type DBDir struct {
	name  string
	mode  int
	mtime int64
	owner int
	grp   int
	acl   string
}

func (dir *DBDir) DBInsert(db *sql.DB) error {
	sql := `
	INSERT OR IGNORE INTO dirs (name, mode, mtime, owner, grp, acl, nested)
	VALUES (?,?,?,?,?,?,?)`
	if _, err := db.Exec(sql, dir.name, dir.mode, dir.mtime, dir.owner, dir.grp, dir.acl, NestedCatalog); err != nil {
		return err
	}
	return nil
}

func (dir *DBDir) DBQuery(db *sql.DB) (*DBCursor, error) {
	rows, err := db.Query("SELECT name, mode, mtime, owner, grp, acl FROM dirs")
	if err != nil {
		return nil, err
	}
	return NewDBCursor(rows, &dir.name, &dir.mode, &dir.mtime, &dir.owner, &dir.grp, &dir.acl), nil
}

func (dir *DBDir) DBCount(db *sql.DB) (int, error) {
	count := 0
	rows, err := db.Query("SELECT count(*) FROM dirs")
	if err != nil {
		return count, err
	}
	rows.Next()
	if err := rows.Scan(&count); err != nil {
		log.Error().Err(err).Msg("Failed to get file count")
		return count, err
	}
	log.Debug().Int("Files Grafted", count).Msg("File Count")
	return count, nil
}

type DBDeletion struct {
	name      string
	directory int
	file      int
	link      int
}

func (del *DBDeletion) DBInsert(db *sql.DB) error {
	sql := `
	INSERT INTO deletions (name, directory, file, link)
	VALUES (?,?,?,?)`
	if _, err := db.Exec(sql, del.name, del.directory, del.file, del.link); err != nil {
		return err
	}
	return nil
}

func (del *DBDeletion) DBQuery(db *sql.DB) (*DBCursor, error) {
	rows, err := db.Query("SELECT name, directory, file, link FROM deletions")
	if err != nil {
		return nil, err
	}
	return NewDBCursor(rows, &del.name, &del.directory, &del.file, &del.link), nil
}

func (del *DBDeletion) DBCount(db *sql.DB) (int, error) {
	count := 0
	rows, err := db.Query("SELECT count(*) FROM deletions")
	if err != nil {
		return count, err
	}
	rows.Next()
	if err := rows.Scan(&count); err != nil {
		log.Error().Err(err).Msg("Failed to get file count")
		return count, err
	}
	log.Debug().Int("Files Grafted", count).Msg("File Count")
	return count, nil
}

type DB interface {
	BackupDatabase(destFilename string) error
	Teardown(removeGraftDb bool) error
	GetPath() string
	InsertFile(name, src string, mode int, mtime int64, owner, group int, size int64, hashes, checksum string, srcInfo fs.FileInfo, external int, alternateBucket bool) error
	InsertUpdatedFile(name, src string, mode int, mtime int64, owner, group int, size int64, hashes, checksum string, srcInfo fs.FileInfo, external int, alternateBucket bool) error
	RemoveFile(name string) error
	UpdateFileHashes(name, hashes string) error
	InsertDir(name string, mode int, mtime int64, owner, group int, acl string) error
	InsertLink(name, target string, mtime int64, owner, group, skip int) error
	ReplaceLink(name, target string, mtime int64, owner, group, skip int) error
	UpdateLinkTarget(name, target string) error
	RemoveLink(name string) error
	InsertDelete(name string, isDir, isFile, isLink int) error
	InsertPurge(name string, alternateBucket bool)
	QueryFiles() ([]string, error)
	QueryFilesAvgSize() (float64, error)
	QueryFilesFullData() ([]DBFile, error)
	QueryUploadFiles() []UploadFile
	QueryLinks() ([]string, error)
	QueryLinksFullData() ([]DBLink, error)
	QueryDirs() ([]string, error)
	QueryDirsFullData() ([]DBDir, error)
	QueryDeletes() ([]string, []string, []string, error)
	QueryDeletesFullData() ([]DBDeletion, error)
	QueryPurges() []PurgeFile
	DBCounts() (int, int, int, int, error)
	IsDatabaseEmpty() (bool, error)
	CopyInDatabase(database DB) error
	UpdateFilesTableWithUpdatedFiles() error
	FileNameClashes() ([]string, error)
}

type CvmfsDB struct {
	db                 *sql.DB
	uploadFiles        []UploadFile
	updatedUploadFiles []UploadFile
	path               string
	purgeList          []PurgeFile
	containingDir      string
}

type S3File interface {
	UseAlternateBucket() bool
}

type PurgeFile struct {
	PathStr         string
	AlternateBucket bool
}

func (pFile PurgeFile) UseAlternateBucket() bool {
	return pFile.AlternateBucket
}

// Can have these be sub functions later, currently just exporting them as accessible fields
type UploadFile struct {
	SrcPathString   string
	DestPathString  string
	FileSize        int64
	Modtime         int64
	SrcInfo         fs.FileInfo
	Owner           int
	Group           int
	Mode            int
	Checksum        string
	AlternateBucket bool
}

func (uFile UploadFile) UseAlternateBucket() bool {
	return uFile.AlternateBucket
}

//go:embed schema.sql
var sqlSchema string

// Create a new database to be used with the grafting tool
func NewCvmfsGraftingDB() (*CvmfsDB, error) {
	dbDir, err := os.MkdirTemp("", CvmfsRsyncDBDirPrefix)
	if err != nil {
		log.Error().Err(err).Msg("Issue creating db dir")
		return nil, err
	}
	dbf, err := os.Create(filepath.Join(dbDir, CvmfsRsyncDBName))
	if err != nil {
		log.Error().Err(err).Msg("Issue creating db")
		return nil, err
	}
	err = dbf.Close()
	if err != nil {
		log.Error().Err(err).Msg("Issue closing db")
		return nil, err
	}
	sqlDb, err := sql.Open("sqlite3", dbf.Name())
	if err != nil {
		log.Error().Err(err).Str("Path", dbf.Name()).Msg("Issue opening file")
		return nil, err
	}
	if err := sqlDb.Ping(); err != nil {
		log.Error().Err(err).Str("Path", dbf.Name()).Msg("Issue Pinging db")
		return nil, err
	}
	if _, err := sqlDb.Exec(sqlSchema); err != nil {
		log.Error().Err(err).Str("Path", dbf.Name()).Msg("Issue executing sql schema")
		return nil, err
	}

	return &CvmfsDB{sqlDb, []UploadFile{}, []UploadFile{}, dbf.Name(), []PurgeFile{}, dbDir}, nil
}

func (db *CvmfsDB) BackupDatabase(destFilename string) error {
	destPath := filepath.Join(".", destFilename)
	destFile, err := os.Create(destPath)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create destination db file")
		return err
	}
	destFile.Close()

	destDb, err := sql.Open("sqlite3", destPath)
	if err != nil {
		log.Error().Err(err).Str("Path", destPath).Msg("Issue opening file")
		return err
	}
	destConn, err := destDb.Conn(context.Background())
	if err != nil {
		return err
	}
	srcConn, err := db.db.Conn(context.Background())
	if err != nil {
		return err
	}

	return destConn.Raw(func(destConn interface{}) error {
		return srcConn.Raw(func(srcConn interface{}) error {
			destSQLiteConn, ok := destConn.(*sqlite3.SQLiteConn)
			if !ok {
				return fmt.Errorf("can't convert destination connection to SQLiteConn")
			}

			srcSQLiteConn, ok := srcConn.(*sqlite3.SQLiteConn)
			if !ok {
				return fmt.Errorf("can't convert source connection to SQLiteConn")
			}

			b, err := destSQLiteConn.Backup("main", srcSQLiteConn, "main")
			if err != nil {
				return fmt.Errorf("error initializing SQLite backup: %w", err)
			}

			done, err := b.Step(-1)
			if !done {
				return fmt.Errorf("step of -1, but not done")
			}
			if err != nil {
				return fmt.Errorf("error in stepping backup: %w", err)
			}

			err = b.Finish()
			if err != nil {
				return fmt.Errorf("error finishing backup: %w", err)
			}

			return err
		})
	})
}

// Teardown created objects of database
func (db *CvmfsDB) Teardown(removeGraftDb bool) error {
	if err := db.db.Close(); err != nil {
		log.Error().Err(err).Msg("Failed to close db. db not removed.")
		return err
	}
	if removeGraftDb {
		if err := os.RemoveAll(db.containingDir); err != nil {
			log.Error().Err(err).Msg("Failed to remove db")
			return err
		}
	} else {
		log.Info().Str("Cvmfs Rsync Directory", db.containingDir).Msg("Did not remove cvmfs_rsync processing database due to error.")
	}
	return nil
}

// Get db path
func (db *CvmfsDB) GetPath() string {
	return db.path
}

func (db *CvmfsDB) InsertFile(name, src string, mode int, mtime int64, owner, group int, size int64, hashes, checksum string, srcInfo fs.FileInfo, external int, alternateBucket bool) error {
	file := DBFile{name, mode, mtime, owner, group, size, hashes, external}
	if err := file.DBInsert(db.db); err != nil {
		return err
	}
	db.uploadFiles = append(
		db.uploadFiles,
		UploadFile{
			SrcPathString:   src,
			DestPathString:  name,
			FileSize:        size,
			Modtime:         mtime,
			SrcInfo:         srcInfo,
			Owner:           owner,
			Group:           group,
			Mode:            mode,
			Checksum:        checksum,
			AlternateBucket: alternateBucket,
		})
	return nil
}

func (db *CvmfsDB) InsertUpdatedFile(name, src string, mode int, mtime int64, owner, group int, size int64, hashes, checksum string, srcInfo fs.FileInfo, external int, alternateBucket bool) error {
	if _, err := db.db.Exec("INSERT INTO updatedFiles VALUES (?,?,?,?,?,?,?,?,?)", name, mode, mtime, owner, group, size, hashes, external, FilesUncompressed); err != nil {
		log.Error().Err(err).Str("Path", name).Msg("Failed to insert updated file")
		return err
	}
	db.updatedUploadFiles = append(
		db.updatedUploadFiles,
		UploadFile{
			SrcPathString:   src,
			DestPathString:  name,
			FileSize:        size,
			Modtime:         mtime,
			SrcInfo:         srcInfo,
			Checksum:        checksum,
			AlternateBucket: alternateBucket,
		})
	return nil
}

// Remove file from database and upload files
func (db *CvmfsDB) RemoveFile(name string) error {
	if _, err := db.db.Exec("DELETE FROM files WHERE name = ?", name); err != nil {
		log.Error().Err(err).Str("Path", name).Msg("Failed to remove file")
		return err
	}
	remove_index := -1
	for index, uFile := range db.uploadFiles {
		if uFile.DestPathString == name {
			remove_index = index
		}
	}
	if remove_index != -1 {
		db.uploadFiles[remove_index] = db.uploadFiles[len(db.uploadFiles)-1]
		db.uploadFiles = db.uploadFiles[:len(db.uploadFiles)-1]
	}
	return nil
}

// Update a file's hashes
func (db *CvmfsDB) UpdateFileHashes(name, hashes string) error {
	if _, err := db.db.Exec("UPDATE files SET hashes=? WHERE name=?", hashes, name); err != nil {
		log.Error().Err(err).Str("Path", name).Msg("Failed to update file")
		return err
	}
	return nil
}

// Insert a directory to database
func (db *CvmfsDB) InsertDir(name string, mode int, mtime int64, owner, group int, acl string) error {
	dbDir := DBDir{name, mode, mtime, owner, group, acl}
	if err := dbDir.DBInsert(db.db); err != nil {
		log.Error().Err(err).Str("Path", name).Msg("Failed to insert dir")
		return err
	}
	return nil
}

// Insert a symlink to the database
func (db *CvmfsDB) InsertLink(name, target string, mtime int64, owner, group, skip int) error {
	link := DBLink{name, target, mtime, owner, group, skip}
	if err := link.DBInsert(db.db); err != nil {
		log.Error().Err(err).Str("Path", name).Msg("Failed to insert sym")
		return err
	}
	return nil
}

// Replace link with name in the database (Change target/metadata info)
func (db *CvmfsDB) ReplaceLink(name, target string, mtime int64, owner, group, skip int) error {
	link := DBLink{name, target, mtime, owner, group, skip}
	if err := link.DBReplace(db.db); err != nil {
		log.Error().Err(err).Str("Path", name).Msg("Failed to replace sym")
		return err
	}
	return nil
}

// Update a link's target
func (db *CvmfsDB) UpdateLinkTarget(name, target string) error {
	if _, err := db.db.Exec("UPDATE files SET target=? WHERE name=?", target, name); err != nil {
		log.Error().Err(err).Str("Path", name).Msg("Failed to update link")
		return err
	}
	return nil
}

// Remove symlink from database
func (db *CvmfsDB) RemoveLink(name string) error {
	if _, err := db.db.Exec("DELETE FROM links WHERE name = ?", name); err != nil {
		log.Error().Err(err).Str("Path", name).Msg("Failed to replace sym")
		return err
	}
	return nil
}

// Insert a delete to the database
func (db *CvmfsDB) InsertDelete(name string, isDir, isFile, isLink int) error {
	deletion := DBDeletion{name, isDir, isFile, isLink}
	if err := deletion.DBInsert(db.db); err != nil {
		log.Error().Err(err).Str("Path", name).Msg("Failed to insert delete")
		return err
	}
	return nil
}

// Insert a purge to the database
func (db *CvmfsDB) InsertPurge(pathStr string, alternateBucket bool) {
	db.purgeList = append(db.purgeList, PurgeFile{pathStr, alternateBucket})
}

// Query all of the file names in the database
func (db *CvmfsDB) QueryFiles() ([]string, error) {
	var filesCreated []string
	var file DBFile

	rows, err := file.DBQuery(db.db)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		filesCreated = append(filesCreated, file.name)
	}

	if rows.Err() != nil {
		log.Error().Msg("Failed to scan rows from files created query")
	}
	return filesCreated, rows.Err()
}

// Query the file avg size from the database
func (db *CvmfsDB) QueryFilesAvgSize() (float64, error) {
	avgSize := 0.0
	rows, err := db.db.Query("SELECT IFNULL(AVG(size), 0.0) FROM files")
	if err != nil {
		log.Error().Msg("Failed to Query from files created")
		return avgSize, err
	}
	rows.Next()
	if err := rows.Scan(&avgSize); err != nil {
		log.Error().Msg("Failed to scan count")
		return avgSize, err
	}
	log.Debug().Float64("Average Size", avgSize).Msg("Average File Size")
	return avgSize, nil
}

// Query all of the database files with their information
func (db *CvmfsDB) QueryFilesFullData() ([]DBFile, error) {
	var dbFiles []DBFile

	var file DBFile
	rows, err := file.DBQuery(db.db)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		dbFiles = append(dbFiles, file)
	}

	if rows.Err() != nil {
		log.Error().Msg("Failed to scan rows from files created query")
	}

	return dbFiles, rows.Err()
}

// Query a list of the files in the UploadFile structure
func (db *CvmfsDB) QueryUploadFiles() []UploadFile {
	return db.uploadFiles
}

// Query all of the link names in the database
func (db *CvmfsDB) QueryLinks() ([]string, error) {
	var linksCreated []string
	var dbLink DBLink
	rows, err := dbLink.DBQuery(db.db)

	if err != nil {
		log.Error().Msg("Failed to Query from links created")
		return nil, err
	}

	for rows.Next() {
		linkStr := fmt.Sprintf("%s : %s", dbLink.name, dbLink.target)
		linksCreated = append(linksCreated, linkStr)
	}

	if rows.Err() != nil {
		log.Error().Msg("Failed to scan rows from links created query")
	}

	return linksCreated, rows.Err()
}

// Query all of the database links with their information
func (db *CvmfsDB) QueryLinksFullData() ([]DBLink, error) {
	var dbLinks []DBLink
	var dbLink DBLink
	rows, err := dbLink.DBQuery(db.db)

	if err != nil {
		log.Error().Msg("Failed to Query from links created")
		return nil, err
	}
	for rows.Next() {
		dbLinks = append(dbLinks, dbLink)
	}

	if rows.Err() != nil {
		log.Error().Msg("Failed to scan rows from links created query")
	}

	return dbLinks, rows.Err()
}

// Query all of the dir names in the database
func (db *CvmfsDB) QueryDirs() ([]string, error) {
	var dirsCreated []string

	var dbDir DBDir
	rows, err := dbDir.DBQuery(db.db)

	if err != nil {
		log.Error().Msg("Failed to Query from dirs created")
		return nil, err
	}

	for rows.Next() {
		dirsCreated = append(dirsCreated, dbDir.name)
	}

	if rows.Err() != nil {
		log.Error().Msg("Failed to scan rows from dirs created query")
	}

	return dirsCreated, rows.Err()
}

// Query all of the database dirs with their information
func (db *CvmfsDB) QueryDirsFullData() ([]DBDir, error) {
	var dbDirs []DBDir
	var dbDir DBDir
	rows, err := dbDir.DBQuery(db.db)

	if err != nil {
		log.Error().Msg("Failed to Query from dirs created")
		return nil, err
	}

	for rows.Next() {
		dbDirs = append(dbDirs, dbDir)
	}

	if rows.Err() != nil {
		log.Error().Msg("Failed to scan rows from dirs created query")
	}

	return dbDirs, rows.Err()
}

// Query all of the delete names in the database, ordered files, dirs, links
func (db *CvmfsDB) QueryDeletes() ([]string, []string, []string, error) {
	var filesDeleted []string
	var dirsDeleted []string
	var linksDeleted []string

	var dbDel DBDeletion

	rows, err := dbDel.DBQuery(db.db)
	if err != nil {
		log.Error().Msg("Failed to Query from deletions")
		return nil, nil, nil, err
	}

	for rows.Next() {
		if dbDel.file == 1 {
			filesDeleted = append(filesDeleted, dbDel.name)
		} else if dbDel.directory == 1 {
			dirsDeleted = append(dirsDeleted, dbDel.name)
		} else if dbDel.link == 1 {
			linksDeleted = append(linksDeleted, dbDel.name)
		}
	}

	if rows.Err() != nil {
		log.Error().Msg("Failed to scan rows from deletions query")
	}

	return filesDeleted, dirsDeleted, linksDeleted, rows.Err()
}

// Query all of the database deletes with their information
func (db *CvmfsDB) QueryDeletesFullData() ([]DBDeletion, error) {
	var dbDeletions []DBDeletion
	var dbDel DBDeletion

	rows, err := dbDel.DBQuery(db.db)
	if err != nil {
		log.Error().Msg("Failed to Query from deletions")
		return nil, err
	}
	for rows.Next() {
		dbDeletions = append(dbDeletions, dbDel)
	}

	if rows.Err() != nil {
		log.Error().Msg("Failed to scan rows from deletions query")
	}

	return dbDeletions, rows.Err()
}

// Query all of the purge names in the database
func (db *CvmfsDB) QueryPurges() []PurgeFile {
	return db.purgeList
}

func (db *CvmfsDB) DBCounts() (int, int, int, int, error) {
	var file DBFile
	var dir DBDir
	var link DBLink
	var del DBDeletion
	numFiles, err := file.DBCount(db.db)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	numDirs, err := dir.DBCount(db.db)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	numLinks, err := link.DBCount(db.db)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	numDels, err := del.DBCount(db.db)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return numFiles, numDirs, numLinks, numDels, nil
}

// Return if the database has nothing in it
func (db *CvmfsDB) IsDatabaseEmpty() (bool, error) {
	tableList := []string{"files", "dirs", "links", "deletions"}
	for _, tableName := range tableList {
		rows, err := db.db.Query("SELECT count(*) FROM " + tableName)
		if err != nil {
			log.Error().Msg("Failed to Query from " + tableName)
			return false, err
		}
		var count int
		for rows.Next() {
			if err := rows.Scan(&count); err != nil {
				log.Error().Msg("Failed to scan rows from " + tableName)
				return false, err
			}
		}
		if count > 0 {
			return false, nil
		}
	}
	return true, nil
}

// Copy passed in database into db calling the function
func (db *CvmfsDB) CopyInDatabase(database DB) error {
	fileInserts, err := database.QueryFilesFullData()
	if err != nil {
		return err
	}
	dirInserts, err := database.QueryDirsFullData()
	if err != nil {
		return err
	}
	linkInserts, err := database.QueryLinksFullData()
	if err != nil {
		return err
	}
	deleteInserts, err := database.QueryDeletesFullData()
	if err != nil {
		return err
	}

	for _, fileInsert := range fileInserts {
		if err := fileInsert.DBInsert(db.db); err != nil {
			log.Error().Err(err).Str("Path", fileInsert.name).Msg("Failed to insert file")
			return err
		}
	}
	for _, dirInsert := range dirInserts {
		if err := dirInsert.DBInsert(db.db); err != nil {
			log.Error().Err(err).Str("Path", dirInsert.name).Msg("Failed to insert dir")
			return err
		}
	}
	for _, linkInsert := range linkInserts {
		if err := linkInsert.DBInsert(db.db); err != nil {
			log.Error().Err(err).Str("Path", linkInsert.name).Msg("Failed to insert sym")
			return err
		}
	}
	for _, deleteInsert := range deleteInserts {
		if err := deleteInsert.DBInsert(db.db); err != nil {
			log.Error().Err(err).Str("Path", deleteInsert.name).Msg("Failed to insert delete")
			return err
		}
	}
	return nil

}

// Copy passed in database into db calling the function
func (db *CvmfsDB) UpdateFilesTableWithUpdatedFiles() error {
	if _, err := db.db.Exec("INSERT INTO files (name, mode, mtime, owner, grp, size, hashes, internal, compressed) " +
		"SELECT name, mode, mtime, owner, grp, size, hashes, internal, compressed FROM updatedFiles"); err != nil {
		log.Error().Err(err).Msg("Failed to update files")
		return err
	}
	db.uploadFiles = append(db.uploadFiles, db.updatedUploadFiles...)
	return nil
}

func (db *CvmfsDB) FileNameClashes() ([]string, error) {
	updatedFileClashes := []string{}
	rows, err := db.db.Query("SELECT files.name FROM files INNER JOIN links ON files.name=links.name")
	if err != nil {
		log.Error().Err(err).Msg("Failed to join files with links")
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Error().Msg("Failed to scan files join link")
			return nil, err
		}
		updatedFileClashes = append(updatedFileClashes, name)
	}
	rows, err = db.db.Query("SELECT files.name FROM files INNER JOIN dirs ON files.name=dirs.name")
	if err != nil {
		log.Error().Err(err).Msg("Failed to join files with dirs")
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Error().Msg("Failed to scan files join dirs")
			return nil, err
		}
		updatedFileClashes = append(updatedFileClashes, name)
	}
	return updatedFileClashes, nil
}

// Create a file replace object
func CreateFileReplace(name string, mode int, mtime int64, owner int, grp int, size int64, hashes, checksum string, srcInfo fs.FileInfo) DBFileReplace {
	return DBFileReplace{
		name:     name,
		mode:     mode,
		mtime:    mtime,
		owner:    owner,
		grp:      grp,
		size:     size,
		hashes:   hashes,
		checksum: checksum,
		srcInfo:  srcInfo,
	}
}

func (file DBFileReplace) GetName() string {
	return file.name
}

func (file DBFileReplace) GetMode() int {
	return file.mode
}

func (file DBFileReplace) GetMtime() int64 {
	return file.mtime
}

func (file DBFileReplace) GetOwner() int {
	return file.owner
}

func (file DBFileReplace) GetGroup() int {
	return file.grp
}

func (file DBFileReplace) GetSize() int64 {
	return file.size
}

func (file *DBFileReplace) SetHashes(hashes string) {
	file.hashes = hashes
}

func (file DBFileReplace) GetHashes() string {
	return file.hashes
}

func (file DBFileReplace) GetChecksum() string {
	return file.checksum
}

func (file DBFileReplace) GetSrcInfo() fs.FileInfo {
	return file.srcInfo
}

func (file DBFileReplace) GetAlternateBucket() bool {
	return file.alternateBucket
}

// Create a link replace object
func CreateLinkReplace(name string, target string, mtime int64, owner int, grp int, skip int) DBLinkReplace {
	return DBLinkReplace{
		name:   name,
		target: target,
		mtime:  mtime,
		owner:  owner,
		grp:    grp,
		skip:   skip,
	}
}

func (link DBLinkReplace) GetName() string {
	return link.name
}

func (link DBLinkReplace) GetTarget() string {
	return link.target
}

func (link DBLinkReplace) GetMtime() int64 {
	return link.mtime
}

func (link DBLinkReplace) GetOwner() int {
	return link.owner
}

func (link DBLinkReplace) GetGroup() int {
	return link.grp
}

func (link DBLinkReplace) GetSkip() int {
	return link.skip
}

func (file DBFile) GetName() string {
	return file.name
}

func (file DBFile) GetOwner() int {
	return file.owner
}

func (file DBFile) GetGroup() int {
	return file.grp
}

func (link DBLink) GetName() string {
	return link.name
}

func (link DBLink) GetOwner() int {
	return link.owner
}

func (link DBLink) GetGroup() int {
	return link.grp
}

func (dir DBDir) GetName() string {
	return dir.name
}

func (dir DBDir) GetOwner() int {
	return dir.owner
}

func (dir DBDir) GetGroup() int {
	return dir.grp
}
