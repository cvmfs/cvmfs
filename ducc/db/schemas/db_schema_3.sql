
/* TODO: Consider using WIHOUT_ROWID. Chcek performance  */
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

CREATE INDEX IF NOT EXISTS "wishes_source_idx" ON "wishes" ("source");

CREATE TABLE IF NOT EXISTS "images" (
    id TEXT PRIMARY KEY,
    digest TEXT,
    tag TEXT,
    registry_scheme TEXT NOT NULL,
    registry_hostname TEXT NOT NULL,
    repository TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "wish_image" (
    wish_id TEXT NOT NULL,
    image_id TEXT NOT NULL,

    FOREIGN KEY (wish_id) REFERENCES wishes (id) ON DELETE CASCADE,
    FOREIGN KEY (image_id) REFERENCES images (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "manifests" (
    image_id TEXT NOT NULL,
    file_digest TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    media_type TEXT NOT NULL,

    config_digest TEXT NOT NULL,
    config_media_type TEXT NOT NULL,
    config_size INTEGER NOT NULL,

    PRIMARY KEY (image_id),
    FOREIGN KEY (image_id)  REFERENCES images (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "manifest_layers" (
    manifest_id TEXT NOT NULL,
    layer_number INTEGER NOT NULL,
    digest TEXT NOT NULL,
    media_type TEXT NOT NULL,
    size INTEGER NOT NULL,

    PRIMARY KEY (manifest_id, layer_number),
    FOREIGN KEY (manifest_id) REFERENCES manifests (image_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "tasks" (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "task_logs"(
    task_id TEXT NOT NULL,
    severity INTEGER NOT NULL,
    message TEXT NOT NULL,
    timestamp TEXT NOT NULL,

    FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "task_relations" (
    task1_id TEXT NOT NULL,
    task2_id TEXT NOT NULL,
    relation TEXT NOT NULL,

    PRIMARY KEY (task1_id, task2_id, relation),
    FOREIGN KEY (task1_id) REFERENCES tasks (id) ON DELETE CASCADE,
    FOREIGN KEY (task2_id) REFERENCES tasks (id) ON DELETE CASCADE
);

COMMIT;