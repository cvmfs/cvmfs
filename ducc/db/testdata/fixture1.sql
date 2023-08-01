PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "wishes" (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,

    cvmfs_repository TEXT NOT NULL,
    input_tag TEXT,
    input_tag_wildcard BOOLEAN NOT NULL,
    input_digest TEXT,
    input_repository TEXT NOT NULL,
    input_registry_scheme TEXT NOT NULL,
    input_registry_hostname TEXT NOT NULL,

    create_layers BOOLEAN,
    create_flat BOOLEAN,
    create_podman BOOLEAN,
    create_thin BOOLEAN,

    webhook_enabled BOOLEAN
);
INSERT INTO wishes VALUES('3b971878-56d7-4cf3-8efc-cea93cdfdecc','source','cvmfs','tag',0,NULL,'repository','https','registry',NULL,NULL,NULL,NULL,NULL);
INSERT INTO wishes VALUES('b7748fed-c394-408d-a36a-ac0d74972b6f','source2','cvmfs','tag',0,NULL,'repository','https','registry',NULL,NULL,NULL,NULL,NULL);
INSERT INTO wishes VALUES('9663d5a2-85c0-419a-8d05-677051ee4d8c','source','cvmfs','*',1,NULL,'repository2','https','registry',NULL,NULL,NULL,NULL,NULL);
CREATE TABLE IF NOT EXISTS "images" (
    id TEXT PRIMARY KEY,
    digest TEXT,
    tag TEXT,
    registry_scheme TEXT NOT NULL,
    registry_hostname TEXT NOT NULL,
    repository TEXT NOT NULL
);
INSERT INTO images VALUES('b0877660-18bb-4c5c-9af5-a8cd886ebded',NULL,'tag','https','registry','repository');
INSERT INTO images VALUES('af0d2f66-638a-44df-bf10-85d984a36385',NULL,'latest','https','registry','repository2');
INSERT INTO images VALUES('a01ff3c1-645e-44d8-ab44-38eccbc80957',NULL,'debug','https','registry','repository2');
CREATE TABLE IF NOT EXISTS "wish_image" (
    wish_id TEXT NOT NULL,
    image_id TEXT NOT NULL,

    FOREIGN KEY (wish_id) REFERENCES wishes (id) ON DELETE CASCADE,
    FOREIGN KEY (image_id) REFERENCES images (id) ON DELETE CASCADE
);
INSERT INTO wish_image VALUES('3b971878-56d7-4cf3-8efc-cea93cdfdecc','b0877660-18bb-4c5c-9af5-a8cd886ebded');
INSERT INTO wish_image VALUES('b7748fed-c394-408d-a36a-ac0d74972b6f','b0877660-18bb-4c5c-9af5-a8cd886ebded');
INSERT INTO wish_image VALUES('9663d5a2-85c0-419a-8d05-677051ee4d8c','af0d2f66-638a-44df-bf10-85d984a36385');
INSERT INTO wish_image VALUES('9663d5a2-85c0-419a-8d05-677051ee4d8c','a01ff3c1-645e-44d8-ab44-38eccbc80957');
CREATE INDEX "wishes_source_idx" ON "wishes" ("source");
COMMIT;
