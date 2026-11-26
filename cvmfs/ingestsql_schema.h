/**
 * This file is part of the CernVM File System.
 *
 * Input database schema of `cvmfs_swissknife ingestsql`, shared by the
 * ingestsql reader and the ingestsql_gen writer.
 */

#ifndef CVMFS_INGESTSQL_SCHEMA_H_
#define CVMFS_INGESTSQL_SCHEMA_H_

namespace ingestsql {

// Every hash in files.hashes covers one fixed-size chunk of the file.
static const unsigned kExternalChunkSize = 24 * 1024 * 1024;
static const unsigned kInternalChunkSize = 6 * 1024 * 1024;

static const int kSchemaRevision = 4;

// Statements are executed one by one, so each must be self-contained.  The
// column comments end up in sqlite_master, i.e. `sqlite3 x.db .schema` shows
// them too.
static const char * const kSchema[] = {
    "PRAGMA journal_mode=WAL;",

    "CREATE TABLE IF NOT EXISTS dirs (\n"
    "  name   TEXT    PRIMARY KEY,           -- repository path\n"
    "  mode   INTEGER NOT NULL DEFAULT 493,  -- permission bits, 493 = 0755;"
    " masked to 0777\n"
    "  mtime  INTEGER NOT NULL DEFAULT 0,    -- nanoseconds since the epoch\n"
    "  owner  INTEGER NOT NULL DEFAULT 0,    -- uid\n"
    "  grp    INTEGER NOT NULL DEFAULT 0,    -- gid\n"
    "  acl    TEXT    NOT NULL DEFAULT '',   -- POSIX access ACL in text form,"
    " e.g.\n"
    "                                        --"
    " 'user::rwx,user:1000:r-x,group::r-x,mask::r-x,other::r-x'\n"
    "  nested INTEGER DEFAULT 1              -- 1: directory is a nested"
    " catalog mountpoint\n"
    ");",

    "CREATE TABLE IF NOT EXISTS files (\n"
    "  name       TEXT    PRIMARY KEY,          -- repository path\n"
    "  mode       INTEGER NOT NULL DEFAULT 420, -- permission bits, 420 = 0644;"
    " masked to 0777\n"
    "  mtime      INTEGER NOT NULL DEFAULT 0,   -- nanoseconds since the"
    " epoch\n"
    "  owner      INTEGER NOT NULL DEFAULT 0,   -- uid\n"
    "  grp        INTEGER NOT NULL DEFAULT 0,   -- gid\n"
    "  size       INTEGER NOT NULL DEFAULT 0,   -- bytes\n"
    "  hashes     TEXT    NOT NULL DEFAULT '',  -- comma-separated hex SHA-1,"
    " one per chunk:\n"
    "                                           --   24 MiB chunks if"
    " internal=0 (SHA-1 of the raw data),\n"
    "                                           --   6 MiB chunks if"
    " internal=1 (object must already be in the CAS);\n"
    "                                           --   an empty file has one"
    " chunk\n"
    "  internal   INTEGER NOT NULL DEFAULT 0,   -- 0: external data, 1: data"
    " in the repository CAS\n"
    "  compressed INTEGER NOT NULL DEFAULT 0    -- 0: default (zlib if"
    " internal), 1: none, 2: zlib (internal only)\n"
    ");",

    "CREATE TABLE IF NOT EXISTS links (\n"
    "  name                TEXT    PRIMARY KEY,         -- repository path\n"
    "  target              TEXT    NOT NULL DEFAULT '', -- symlink target\n"
    "  mtime               INTEGER NOT NULL DEFAULT 0,  -- nanoseconds since"
    " the epoch\n"
    "  owner               INTEGER NOT NULL DEFAULT 0,  -- uid\n"
    "  grp                 INTEGER NOT NULL DEFAULT 0,  -- gid\n"
    "  skip_if_file_or_dir INTEGER NOT NULL DEFAULT 0   -- 1: keep an existing"
    " file or directory at name\n"
    ");",

    "CREATE TABLE IF NOT EXISTS deletions (\n"
    "  name      TEXT PRIMARY KEY,          -- repository path, processed"
    " before additions\n"
    "  directory INTEGER NOT NULL DEFAULT 0, -- 1: delete if it is a directory"
    " (recursively)\n"
    "  file      INTEGER NOT NULL DEFAULT 0, -- 1: delete if it is a regular"
    " file\n"
    "  link      INTEGER NOT NULL DEFAULT 0  -- 1: delete if it is a symlink\n"
    ");",

    "CREATE TABLE IF NOT EXISTS properties (\n"
    "  key   TEXT PRIMARY KEY, -- schema_revision; completed_graft (set by"
    " ingestsql -Z)\n"
    "  value TEXT NOT NULL\n"
    ");",

    "INSERT INTO properties VALUES ('schema_revision', '4')"
    " ON CONFLICT DO NOTHING;",

    nullptr};

static const char * const kSchemaNotes =
    "-- Input schema of cvmfs_server ingestsql (schema_revision 4).\n"
    "--\n"
    "-- * Names are repository paths; a leading '/' is optional.  The lease\n"
    "--   path defaults to the longest common prefix of all names.  Missing\n"
    "--   directories below it are created with default attributes, but\n"
    "--   its parent must already exist in the repository.\n"
    "-- * ingestsql uploads no data: files.hashes must reference objects\n"
    "--   that already exist, either in the repository CAS (internal=1) or\n"
    "--   on the external data server (internal=0, CVMFS_EXTERNAL_URL).\n"
    "-- * setuid, setgid and sticky bits are dropped.  Hardlinks, device\n"
    "--   files, FIFOs and sockets cannot be expressed.\n"
    "-- * A directory with nested=1 becomes a nested catalog; ingestsql\n"
    "--   creates its .cvmfscatalog marker.\n";

}  // namespace ingestsql

#endif  // CVMFS_INGESTSQL_SCHEMA_H_
