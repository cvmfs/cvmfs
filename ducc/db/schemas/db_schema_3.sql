
/* TODO: Consider using WIHOUT_ROWID. Check performance  */
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

    update_interval_sec INTEGER,
    webhook_enabled BOOLEAN
);

CREATE INDEX IF NOT EXISTS "wishes_source_idx" ON "wishes" ("source");

CREATE TABLE IF NOT EXISTS "images" (
    id TEXT PRIMARY KEY,
    digest TEXT,
    tag TEXT,
    registry_scheme TEXT NOT NULL,
    registry_hostname TEXT NOT NULL,
    repository TEXT NOT NULL,

    manifest_digest TEXT,
    manifest_last_fetched TEXT
);

CREATE TABLE IF NOT EXISTS "wish_image" (
    wish_id TEXT NOT NULL,
    image_id TEXT NOT NULL,

    FOREIGN KEY (wish_id) REFERENCES wishes (id) ON DELETE CASCADE,
    FOREIGN KEY (image_id) REFERENCES images (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "tasks" (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT NOT NULL,

    title TEXT NOT NULL,

    created_timestamp TEXT NOT NULL,
    start_timestamp TEXT,
    done_timestamp TEXT
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

CREATE TABLE IF NOT EXISTS "triggers" (
    id TEXT PRIMARY KEY,

    action TEXT NOT NULL,
    object_id TEXT NOT NULL,
    
    timestamp TEXT NOT NULL,
    reason TEXT NOT NULL,
    details TEXT NOT NULL,
    
    task_id TEXT,

    FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE CASCADE
);

COMMIT;