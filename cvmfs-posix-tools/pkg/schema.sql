PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS dirs (
	name  TEXT    PRIMARY KEY,
	mode  INTEGER NOT NULL DEFAULT 493, -- 0755 in octal
	mtime INTEGER NOT NULL DEFAULT (unixepoch()), -- Unix seconds
	owner INTEGER NOT NULL DEFAULT 0,
	grp   INTEGER NOT NULL DEFAULT 0,
	acl   TEXT    NOT NULL DEFAULT "",
	nested INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS updatedFiles (
	name     TEXT    PRIMARY KEY,
	mode     INTEGER NOT NULL DEFAULT 420, -- 0644 in octal
	mtime    INTEGER NOT NULL DEFAULT (unixepoch()), -- Unix seconds
	owner    INTEGER NOT NULL DEFAULT 0,
	grp      INTEGER NOT NULL DEFAULT 0,
	size     INTEGER NOT NULL DEFAULT 0,
	hashes   TEXT    NOT NULL DEFAULT "",
	internal INTEGER NOT NULL DEFAULT 0,
	compressed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS files (
	name     TEXT    PRIMARY KEY,
	mode     INTEGER NOT NULL DEFAULT 420, -- 0644 in octal
	mtime    INTEGER NOT NULL DEFAULT (unixepoch()), -- Unix seconds
	owner    INTEGER NOT NULL DEFAULT 0,
	grp      INTEGER NOT NULL DEFAULT 0,
	size     INTEGER NOT NULL DEFAULT 0,
	hashes   TEXT    NOT NULL DEFAULT "",
	internal INTEGER NOT NULL DEFAULT 0,
	compressed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS links (
	name                TEXT    PRIMARY KEY,
	target              TEXT    NOT NULL DEFAULT "",
	mtime               INTEGER NOT NULL DEFAULT (unixepoch()), -- Unix seconds
	owner               INTEGER NOT NULL DEFAULT 0,
	grp                 INTEGER NOT NULL DEFAULT 0,
	skip_if_file_or_dir INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS deletions (
	name      TEXT PRIMARY KEY,
	directory INTEGER NOT NULL DEFAULT 0, -- Boolean to indicate if the item is a directory
	file      INTEGER NOT NULL DEFAULT 0, -- Boolean to indicate if the item is a file
	link      INTEGER NOT NULL DEFAULT 0  -- Boolean to indicate if the item is a link
);

-- Table used by CVMFS to store different properties such as schema version and revision.
CREATE TABLE IF NOT EXISTS properties (
	key   TEXT PRIMARY KEY,
	value TEXT
);

INSERT INTO properties VALUES
	("schema", "1.0"),
	("schema_revision", "4")
;