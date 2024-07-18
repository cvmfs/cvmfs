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
CREATE TABLE IF NOT EXISTS "images" (
    id TEXT PRIMARY KEY,
    digest TEXT,
    tag TEXT,
    registry_scheme TEXT NOT NULL,
    registry_hostname TEXT NOT NULL,
    repository TEXT NOT NULL
);
INSERT INTO images VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',NULL,'latest','https','registry.hub.docker.com','atlas/rucio-clients');
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
INSERT INTO manifests VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10','sha256:f998e6372fc338c72a755c88b2bc4409e99245abb605b03aafa1697a2b1df120',2,'application/vnd.docker.distribution.manifest.v2+json','sha256:4c1abb0015175a767b6832e9697b9860377ba68460a565221aab2bc84dc7aa59','application/vnd.docker.container.image.v1+json',7218);
CREATE TABLE IF NOT EXISTS "manifest_layers" (
    manifest_id TEXT NOT NULL,
    layer_number INTEGER NOT NULL,
    digest TEXT NOT NULL,
    media_type TEXT NOT NULL,
    size INTEGER NOT NULL,

    PRIMARY KEY (manifest_id, layer_number),
    FOREIGN KEY (manifest_id) REFERENCES manifests (image_id) ON DELETE CASCADE
);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',0,'sha256:2d473b07cdd5f0912cd6f1a703352c82b512407db6b05b43f2553732b55df3bc','application/vnd.docker.image.rootfs.diff.tar.gzip',76097157);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',1,'sha256:9f26fd9d8fd9175526d4f78a749789ae2deeca98d788d7531489bdcdc20887de','application/vnd.docker.image.rootfs.diff.tar.gzip',6546833);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',2,'sha256:4d63c89a744b7dc6c37926c6346b25a68710f3c94f2e24428d57e8c53ddb29bf','application/vnd.docker.image.rootfs.diff.tar.gzip',58858217);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',3,'sha256:e6975fa5f78d58d5960b36c92bf507f3058cc397e2bb94d1163d9fde1a1bb5d8','application/vnd.docker.image.rootfs.diff.tar.gzip',123903651);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',4,'sha256:cffc290ce5cc5f6984bbdb62f1da9a785b1b9cede29159ba40db5fcbf3caec5a','application/vnd.docker.image.rootfs.diff.tar.gzip',8742549);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',5,'sha256:10b212f1203d4bbb0154f8e6ab85b371b5b4468c583af3c661edf778529a35bb','application/vnd.docker.image.rootfs.diff.tar.gzip',2338);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',6,'sha256:4f4fb700ef54461cfa02571ae0db9a0dc1e0cdb5577484a6d75e68dc38e8acc1','application/vnd.docker.image.rootfs.diff.tar.gzip',32);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',7,'sha256:1b9c7bcf2dafacdf4d336bfaeb4fa382f924e75210fcd82a4ad31e2547d0894c','application/vnd.docker.image.rootfs.diff.tar.gzip',676);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',8,'sha256:c3b48b44328cf9d0d6fb5b57f08242687ec4065d302ed080b5f3587bb93d01a5','application/vnd.docker.image.rootfs.diff.tar.gzip',411);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',9,'sha256:581ae022a7697b821cbb930c9e14b76c35dd6390a4c354cdcae1e8c248f2394a','application/vnd.docker.image.rootfs.diff.tar.gzip',379);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',10,'sha256:b4897621d6338127777d0267e4fa019f8e79d0176afd814369e8c24c7cd88206','application/vnd.docker.image.rootfs.diff.tar.gzip',394);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',11,'sha256:40a0b97495ad7f4e1ed305e5b8b6609aa932ae94073f8ba78108b1f73d6f6136','application/vnd.docker.image.rootfs.diff.tar.gzip',92587317);
INSERT INTO manifest_layers VALUES('44e75c94-8f61-4c5a-9c3b-fdde888b3c10',12,'sha256:ea9530c1388cd40b8239bf0d04192f87c7f6302d6abe991320f6ab22878c7421','application/vnd.docker.image.rootfs.diff.tar.gzip',8269748);
CREATE TABLE IF NOT EXISTS "tasks" (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT NOT NULL
);
INSERT INTO tasks VALUES('99bc95a5-30b2-447f-8045-9e00903ea0dd','CREATE_FLAT','DONE','FAILURE');
INSERT INTO tasks VALUES('d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','CREATE_CHAIN','DONE','SUCCESS');
INSERT INTO tasks VALUES('01282518-8dcc-462d-9825-22721d971f95','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('3a75394a-3676-4ab6-9b51-b9a52d20c7b4','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('0d951ba7-aea8-4914-a25b-f2ad654fe835','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('171006f0-e774-45f3-8380-0bcc3ac79af2','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('ac3bd207-200d-46ae-ba79-5dc36fa55590','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('0123e789-2eae-4b60-92ab-05fdd161e9bf','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('7d92c044-dcc1-4fbb-9187-8773ec119a24','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('9d6bdc65-d2d0-402d-938f-aa05e93b8345','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('6ba3ab27-5d39-4d1d-8b83-381746287efa','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('b337ecf9-7520-4a5d-8edd-76bbd79b6f7a','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('65ec1c87-3eca-46c5-9e28-0374151e1203','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('033f8668-460a-4728-b9f1-6fd88ef69b87','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('303ad85a-4841-490d-b13a-5dab3ded9e60','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('782169f2-baf3-436c-a89b-5280de6dec8a','CREATE_SINGULARITY_FILES','DONE','FAILURE');
CREATE TABLE IF NOT EXISTS "task_logs"(
    task_id TEXT NOT NULL,
    severity INTEGER NOT NULL,
    message TEXT NOT NULL,
    timestamp TEXT NOT NULL,

    FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE CASCADE
);
INSERT INTO task_logs VALUES('01282518-8dcc-462d-9825-22721d971f95',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.529777804Z');
INSERT INTO task_logs VALUES('3a75394a-3676-4ab6-9b51-b9a52d20c7b4',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.532339495Z');
INSERT INTO task_logs VALUES('0d951ba7-aea8-4914-a25b-f2ad654fe835',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.534728143Z');
INSERT INTO task_logs VALUES('171006f0-e774-45f3-8380-0bcc3ac79af2',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.537078206Z');
INSERT INTO task_logs VALUES('ac3bd207-200d-46ae-ba79-5dc36fa55590',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.540253104Z');
INSERT INTO task_logs VALUES('0123e789-2eae-4b60-92ab-05fdd161e9bf',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.542811017Z');
INSERT INTO task_logs VALUES('7d92c044-dcc1-4fbb-9187-8773ec119a24',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.545764059Z');
INSERT INTO task_logs VALUES('9d6bdc65-d2d0-402d-938f-aa05e93b8345',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.548543354Z');
INSERT INTO task_logs VALUES('6ba3ab27-5d39-4d1d-8b83-381746287efa',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.551592109Z');
INSERT INTO task_logs VALUES('b337ecf9-7520-4a5d-8edd-76bbd79b6f7a',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.554564604Z');
INSERT INTO task_logs VALUES('65ec1c87-3eca-46c5-9e28-0374151e1203',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.557199778Z');
INSERT INTO task_logs VALUES('033f8668-460a-4728-b9f1-6fd88ef69b87',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.559766216Z');
INSERT INTO task_logs VALUES('303ad85a-4841-490d-b13a-5dab3ded9e60',3,'Chain link already exists in CVMFS','2023-08-04T14:07:30.562524945Z');
INSERT INTO task_logs VALUES('99bc95a5-30b2-447f-8045-9e00903ea0dd',4,'Waiting to start task','2023-08-04T14:07:30.566307973Z');
INSERT INTO task_logs VALUES('d6fcf055-d212-4cd0-bde4-48ddbd0a96e8',4,'Waiting to start task','2023-08-04T14:07:30.567857211Z');
INSERT INTO task_logs VALUES('99bc95a5-30b2-447f-8045-9e00903ea0dd',3,'Started task','2023-08-04T14:07:30.571955884Z');
INSERT INTO task_logs VALUES('99bc95a5-30b2-447f-8045-9e00903ea0dd',3,'Started creating chain, waiting for it to finish','2023-08-04T14:07:30.574638721Z');
INSERT INTO task_logs VALUES('d6fcf055-d212-4cd0-bde4-48ddbd0a96e8',3,'Started task','2023-08-04T14:07:30.576429211Z');
INSERT INTO task_logs VALUES('d6fcf055-d212-4cd0-bde4-48ddbd0a96e8',3,'Started creating chain links, waiting for them to finish','2023-08-04T14:07:30.577105713Z');
INSERT INTO task_logs VALUES('d6fcf055-d212-4cd0-bde4-48ddbd0a96e8',3,'Successfully created chain links','2023-08-04T14:07:30.578181488Z');
INSERT INTO task_logs VALUES('99bc95a5-30b2-447f-8045-9e00903ea0dd',3,'Successfully created chain','2023-08-04T14:07:30.578248323Z');
INSERT INTO task_logs VALUES('99bc95a5-30b2-447f-8045-9e00903ea0dd',3,'Started singularity files creation, waiting for it to finish','2023-08-04T14:07:30.580556944Z');
INSERT INTO task_logs VALUES('782169f2-baf3-436c-a89b-5280de6dec8a',4,'Waiting to start task','2023-08-04T14:07:30.584251694Z');
INSERT INTO task_logs VALUES('782169f2-baf3-436c-a89b-5280de6dec8a',3,'Started task','2023-08-04T14:07:30.585587742Z');
INSERT INTO task_logs VALUES('782169f2-baf3-436c-a89b-5280de6dec8a',0,'Not implemented','2023-08-04T14:07:30.586257904Z');
INSERT INTO task_logs VALUES('99bc95a5-30b2-447f-8045-9e00903ea0dd',0,'Singularity files creation failed','2023-08-04T14:07:30.587345567Z');
CREATE TABLE IF NOT EXISTS "task_relations" (
    task1_id TEXT NOT NULL,
    task2_id TEXT NOT NULL,
    relation TEXT NOT NULL,

    PRIMARY KEY (task1_id, task2_id, relation),
    FOREIGN KEY (task1_id) REFERENCES tasks (id) ON DELETE CASCADE,
    FOREIGN KEY (task2_id) REFERENCES tasks (id) ON DELETE CASCADE
);
INSERT INTO task_relations VALUES('01282518-8dcc-462d-9825-22721d971f95','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('3a75394a-3676-4ab6-9b51-b9a52d20c7b4','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('0d951ba7-aea8-4914-a25b-f2ad654fe835','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('171006f0-e774-45f3-8380-0bcc3ac79af2','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('ac3bd207-200d-46ae-ba79-5dc36fa55590','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('0123e789-2eae-4b60-92ab-05fdd161e9bf','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('7d92c044-dcc1-4fbb-9187-8773ec119a24','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('9d6bdc65-d2d0-402d-938f-aa05e93b8345','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('6ba3ab27-5d39-4d1d-8b83-381746287efa','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('b337ecf9-7520-4a5d-8edd-76bbd79b6f7a','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('65ec1c87-3eca-46c5-9e28-0374151e1203','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('033f8668-460a-4728-b9f1-6fd88ef69b87','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('303ad85a-4841-490d-b13a-5dab3ded9e60','d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','SUBTASK_OF');
INSERT INTO task_relations VALUES('d6fcf055-d212-4cd0-bde4-48ddbd0a96e8','99bc95a5-30b2-447f-8045-9e00903ea0dd','SUBTASK_OF');
INSERT INTO task_relations VALUES('782169f2-baf3-436c-a89b-5280de6dec8a','99bc95a5-30b2-447f-8045-9e00903ea0dd','SUBTASK_OF');
CREATE INDEX "wishes_source_idx" ON "wishes" ("source");
COMMIT;
