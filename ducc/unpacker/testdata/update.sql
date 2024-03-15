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
    repository TEXT NOT NULL,

    manifest_digest TEXT,
    manifest_last_fetched TEXT
    manifest_list_digest TEXT,
    manifest_list_last_fetched TEXT
);
INSERT INTO images VALUES('d2a0e6bb-f09a-49cb-915a-73a34957be9c',NULL,'23.0.27','https','registry.hub.docker.com','atlas/athena','sha256:ffaf1c4efaf3411454e05d82363dbccf442e2f922536b341d85cf1a3474882a9','2023-08-10T14:01:28.134520497Z',NULL);
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
    result TEXT NOT NULL
);
INSERT INTO tasks VALUES('aadfc6ae-0a03-4ce2-ade3-b0ed0edca0d1','CREATE_FLAT','DONE','FAILURE');
INSERT INTO tasks VALUES('a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','CREATE_CHAIN','DONE','FAILURE');
INSERT INTO tasks VALUES('8b1a9925-ccd3-4a25-92ab-6cbfda77ee9c','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('d19e4bb8-1a4f-49fc-9b9a-909e38b9e911','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('143d5cb6-fcbd-4a7d-be11-0f967cb9235b','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('2258ba27-230d-47fc-988b-e3fab0d5b62f','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('56600328-3a61-432e-a33e-c7747d486b8a','CREATE_CHAIN_LINK','DONE','SKIPPED');
INSERT INTO tasks VALUES('c05d4410-25c3-4861-adce-3a4f654a409f','CREATE_CHAIN_LINK','RUNNING','');
INSERT INTO tasks VALUES('a31f6685-fa68-4ba2-a59d-7bb43cffe18b','DOWNLOAD_BLOB','DONE','SUCCESS');
INSERT INTO tasks VALUES('b13938dd-1086-472c-959e-e9ee0cf4eda6','INGEST_CHAIN_LINK','RUNNING','');
INSERT INTO tasks VALUES('b4a54c5f-db8d-4644-b157-220500e61793','CREATE_CHAIN_LINK','RUNNING','');
INSERT INTO tasks VALUES('dc0ce7d3-9ba5-4002-bb5b-ed45bec555d1','DOWNLOAD_BLOB','DONE','SUCCESS');
INSERT INTO tasks VALUES('f2d95f3b-f5d6-4449-ae82-a0fba60d9a6b','INGEST_CHAIN_LINK','','');
INSERT INTO tasks VALUES('659dae44-6360-4e1f-ab7e-3dbcf0ed0437','CREATE_CHAIN_LINK','RUNNING','');
INSERT INTO tasks VALUES('62edacd1-2986-491e-a833-5f54bff18a1d','DOWNLOAD_BLOB','DONE','SUCCESS');
INSERT INTO tasks VALUES('d71f242b-07f7-4856-88a3-9be4debbca6b','INGEST_CHAIN_LINK','','');
INSERT INTO tasks VALUES('672fc2e5-a644-4687-bcc5-932999b3b55e','CREATE_CHAIN_LINK','RUNNING','');
INSERT INTO tasks VALUES('81ff1c6f-d157-4584-8dc4-6f4c6ecd52c2','DOWNLOAD_BLOB','DONE','SUCCESS');
INSERT INTO tasks VALUES('e1bce77e-aeb2-4dfa-886a-b73f7aeb3041','INGEST_CHAIN_LINK','','');
INSERT INTO tasks VALUES('d10144c7-bb18-4f1c-aa55-b70f7fbd4429','CREATE_CHAIN_LINK','RUNNING','');
INSERT INTO tasks VALUES('3eac4b1f-af2d-474e-ad86-42db941b2233','DOWNLOAD_BLOB','DONE','SUCCESS');
INSERT INTO tasks VALUES('7dcc2563-3b1b-4a68-a96d-aefbbd23f218','INGEST_CHAIN_LINK','','');
INSERT INTO tasks VALUES('aba07806-5074-4ae7-bcb4-46de49264437','CREATE_CHAIN_LINK','RUNNING','');
INSERT INTO tasks VALUES('348475ce-ec98-47b7-ac0b-02bf372375fe','DOWNLOAD_BLOB','DONE','SUCCESS');
INSERT INTO tasks VALUES('75d40437-d196-493e-bbc4-005822d9dc56','INGEST_CHAIN_LINK','','');
INSERT INTO tasks VALUES('f2f2a8b7-dc43-407f-a258-5c49fa3d67b4','CREATE_CHAIN_LINK','RUNNING','');
INSERT INTO tasks VALUES('ee7314b5-8bf0-40e4-a77e-ae38b3fef3c9','INGEST_CHAIN_LINK','','');
INSERT INTO tasks VALUES('9029275f-9fc0-4a85-b0a9-c7e976420392','CREATE_CHAIN_LINK','DONE','FAILURE');
INSERT INTO tasks VALUES('dc547fa2-a5e6-4c41-957d-f7cc3bff22f2','DOWNLOAD_BLOB','','');
INSERT INTO tasks VALUES('38b75513-c972-470a-8bcc-fd2b81690269','CREATE_CHAIN_LINK','DONE','FAILURE');
INSERT INTO tasks VALUES('42978afd-8016-4354-8918-ca354512ae6f','DOWNLOAD_BLOB','','');
INSERT INTO tasks VALUES('e4629143-a214-40da-ac97-78f5c6dbb7ca','INGEST_CHAIN_LINK','','');
INSERT INTO tasks VALUES('c3fbb9fb-cba1-4f58-aa2e-ddc39a24ac5c','CREATE_CHAIN_LINK','DONE','FAILURE');
INSERT INTO tasks VALUES('8e3e106d-9b0f-4d13-a469-f6c50b7a00e9','INGEST_CHAIN_LINK','','');
INSERT INTO tasks VALUES('904132d9-9a2f-491d-96ca-b68370ceef15','CREATE_SINGULARITY_FILES','DONE','FAILURE');
INSERT INTO tasks VALUES('ea391f2f-caf4-423f-8260-c35f35da0ae2','FETCH_OCI_CONFIG','','');
INSERT INTO tasks VALUES('2622611e-ea2e-486f-9cb8-aa5a11f5959e','CREATE_LAYERS','DONE','FAILURE');
INSERT INTO tasks VALUES('5f65deb4-3330-432e-bd25-20134d081aad','INGEST_LAYERS','DONE','FAILURE');
INSERT INTO tasks VALUES('61b69ba8-caf7-4364-a1b9-6310129dd598','CREATE_LAYER','DONE','FAILURE');
INSERT INTO tasks VALUES('712ba0de-ed1e-4bfb-bea4-ddd045e2f6e7','DOWNLOAD_BLOB','','');
INSERT INTO tasks VALUES('94ede230-66e2-412f-a934-64f8d2ea4cbd','CREATE_LAYER','','');
INSERT INTO tasks VALUES('dddf892f-146e-4b4f-ad81-ee612669eb31','INGEST_LAYER','','');
INSERT INTO tasks VALUES('92f42c68-9139-4ec3-9d72-ecf1a68cb55f','CREATE_LAYER','','');
INSERT INTO tasks VALUES('669aadaa-12ec-493a-a4af-aeef30dedb6a','DOWNLOAD_BLOB','','');
INSERT INTO tasks VALUES('0ee87f2b-b79d-45fe-a0b2-68b9e238ca53','INGEST_LAYER','','');
INSERT INTO tasks VALUES('dcae3c2e-e984-476b-8533-3674b2e85600','CREATE_LAYER','','');
INSERT INTO tasks VALUES('62f8edda-78b4-4fc8-b8a5-ebd14c3220a3','DOWNLOAD_BLOB','','');
INSERT INTO tasks VALUES('28a86c2d-e2d2-4e69-a555-acfe4d7f8263','INGEST_LAYER','','');
INSERT INTO tasks VALUES('bf7ce4f4-0ff6-494b-b8d6-98087741f605','WRITE_IMAGE_METADATA','DONE','FAILURE');
INSERT INTO tasks VALUES('8940d412-8efd-4ea1-9458-8dfa87b9db2f','FETCH_OCI_CONFIG','DONE','SUCCESS');
INSERT INTO tasks VALUES('340c6613-0e3f-487c-9dd0-88bf80145161','CREATE_PODMAN','DONE','SUCCESS');
INSERT INTO tasks VALUES('cf1220ef-2cc5-4c9a-badb-c69e6799f633','FETCH_OCI_CONFIG','DONE','SUCCESS');
CREATE TABLE IF NOT EXISTS "task_logs"(
    task_id TEXT NOT NULL,
    severity INTEGER NOT NULL,
    message TEXT NOT NULL,
    timestamp TEXT NOT NULL,

    FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE CASCADE
);
INSERT INTO task_logs VALUES('8b1a9925-ccd3-4a25-92ab-6cbfda77ee9c',3,'Chain link already exists in CVMFS','2023-08-10T14:01:28.137662544Z');
INSERT INTO task_logs VALUES('d19e4bb8-1a4f-49fc-9b9a-909e38b9e911',3,'Chain link already exists in CVMFS','2023-08-10T14:01:28.140261335Z');
INSERT INTO task_logs VALUES('143d5cb6-fcbd-4a7d-be11-0f967cb9235b',3,'Chain link already exists in CVMFS','2023-08-10T14:01:28.142554839Z');
INSERT INTO task_logs VALUES('2258ba27-230d-47fc-988b-e3fab0d5b62f',3,'Chain link already exists in CVMFS','2023-08-10T14:01:28.145012634Z');
INSERT INTO task_logs VALUES('56600328-3a61-432e-a33e-c7747d486b8a',3,'Chain link already exists in CVMFS','2023-08-10T14:01:28.147414648Z');
INSERT INTO task_logs VALUES('d71f242b-07f7-4856-88a3-9be4debbca6b',4,'Waiting to start task','2023-08-10T14:01:28.161490811Z');
INSERT INTO task_logs VALUES('7dcc2563-3b1b-4a68-a96d-aefbbd23f218',4,'Waiting to start task','2023-08-10T14:01:28.175874361Z');
INSERT INTO task_logs VALUES('e1bce77e-aeb2-4dfa-886a-b73f7aeb3041',4,'Waiting to start task','2023-08-10T14:01:28.168488624Z');
INSERT INTO task_logs VALUES('672fc2e5-a644-4687-bcc5-932999b3b55e',4,'Waiting to start task','2023-08-10T14:01:28.177795097Z');
INSERT INTO task_logs VALUES('75d40437-d196-493e-bbc4-005822d9dc56',4,'Waiting to start task','2023-08-10T14:01:28.175992038Z');
INSERT INTO task_logs VALUES('d10144c7-bb18-4f1c-aa55-b70f7fbd4429',4,'Waiting to start task','2023-08-10T14:01:28.181429238Z');
INSERT INTO task_logs VALUES('659dae44-6360-4e1f-ab7e-3dbcf0ed0437',4,'Waiting to start task','2023-08-10T14:01:28.16498965Z');
INSERT INTO task_logs VALUES('c05d4410-25c3-4861-adce-3a4f654a409f',4,'Waiting to start task','2023-08-10T14:01:28.186021746Z');
INSERT INTO task_logs VALUES('b4a54c5f-db8d-4644-b157-220500e61793',4,'Waiting to start task','2023-08-10T14:01:28.159602211Z');
INSERT INTO task_logs VALUES('9029275f-9fc0-4a85-b0a9-c7e976420392',0,'Failed to add "DOWNLOAD_BLOB" task as subtask: database is locked','2023-08-10T14:01:28.193158025Z');
INSERT INTO task_logs VALUES('e4629143-a214-40da-ac97-78f5c6dbb7ca',4,'Waiting to start task','2023-08-10T14:01:28.201137679Z');
INSERT INTO task_logs VALUES('38b75513-c972-470a-8bcc-fd2b81690269',0,'Failed to add "INGEST_CHAIN_LINK" task as subtask: database is locked','2023-08-10T14:01:28.201245842Z');
INSERT INTO task_logs VALUES('8e3e106d-9b0f-4d13-a469-f6c50b7a00e9',4,'Waiting to start task','2023-08-10T14:01:28.206116309Z');
INSERT INTO task_logs VALUES('904132d9-9a2f-491d-96ca-b68370ceef15',0,'Failed to add download config task as subtask: database is locked','2023-08-10T14:01:28.209383464Z');
INSERT INTO task_logs VALUES('ea391f2f-caf4-423f-8260-c35f35da0ae2',4,'Waiting for start','2023-08-10T14:01:28.209504302Z');
INSERT INTO task_logs VALUES('61b69ba8-caf7-4364-a1b9-6310129dd598',0,'Failed to add "DOWNLOAD_BLOB" as subtask: database is locked','2023-08-10T14:01:28.21780547Z');
INSERT INTO task_logs VALUES('28a86c2d-e2d2-4e69-a555-acfe4d7f8263',4,'Waiting for task to start','2023-08-10T14:01:28.234570419Z');
INSERT INTO task_logs VALUES('94ede230-66e2-412f-a934-64f8d2ea4cbd',4,'Waiting for task to start','2023-08-10T14:01:28.234569763Z');
INSERT INTO task_logs VALUES('b13938dd-1086-472c-959e-e9ee0cf4eda6',4,'Waiting to start task','2023-08-10T14:01:28.230445794Z');
INSERT INTO task_logs VALUES('a10190f9-bb2d-4aa8-9bfc-39e3d9eef831',4,'Waiting to start task','2023-08-10T14:01:28.241105315Z');
INSERT INTO task_logs VALUES('f2d95f3b-f5d6-4449-ae82-a0fba60d9a6b',4,'Waiting to start task','2023-08-10T14:01:28.188769089Z');
INSERT INTO task_logs VALUES('92f42c68-9139-4ec3-9d72-ecf1a68cb55f',4,'Waiting for task to start','2023-08-10T14:01:28.234636851Z');
INSERT INTO task_logs VALUES('dddf892f-146e-4b4f-ad81-ee612669eb31',4,'Waiting for task to start','2023-08-10T14:01:28.22516601Z');
INSERT INTO task_logs VALUES('f2f2a8b7-dc43-407f-a258-5c49fa3d67b4',4,'Waiting to start task','2023-08-10T14:01:28.244903331Z');
INSERT INTO task_logs VALUES('aadfc6ae-0a03-4ce2-ade3-b0ed0edca0d1',4,'Waiting to start task','2023-08-10T14:01:28.214472309Z');
INSERT INTO task_logs VALUES('c3fbb9fb-cba1-4f58-aa2e-ddc39a24ac5c',4,'Waiting to start task','2023-08-10T14:01:28.215311416Z');
INSERT INTO task_logs VALUES('0ee87f2b-b79d-45fe-a0b2-68b9e238ca53',4,'Waiting for task to start','2023-08-10T14:01:28.234831198Z');
INSERT INTO task_logs VALUES('dcae3c2e-e984-476b-8533-3674b2e85600',4,'Waiting for task to start','2023-08-10T14:01:28.253291056Z');
INSERT INTO task_logs VALUES('aba07806-5074-4ae7-bcb4-46de49264437',4,'Waiting to start task','2023-08-10T14:01:28.185389891Z');
INSERT INTO task_logs VALUES('5f65deb4-3330-432e-bd25-20134d081aad',0,'Failed to add "CREATE_LAYER" as subtask: database is locked','2023-08-10T14:01:28.268785393Z');
INSERT INTO task_logs VALUES('ee7314b5-8bf0-40e4-a77e-ae38b3fef3c9',4,'Waiting to start task','2023-08-10T14:01:28.190346642Z');
INSERT INTO task_logs VALUES('8940d412-8efd-4ea1-9458-8dfa87b9db2f',4,'Waiting for start','2023-08-10T14:01:28.272053904Z');
INSERT INTO task_logs VALUES('cf1220ef-2cc5-4c9a-badb-c69e6799f633',4,'Waiting for start','2023-08-10T14:01:38.866258555Z');
INSERT INTO task_logs VALUES('bf7ce4f4-0ff6-494b-b8d6-98087741f605',4,'Waiting for task to start','2023-08-10T14:01:46.033146861Z');
INSERT INTO task_logs VALUES('2622611e-ea2e-486f-9cb8-aa5a11f5959e',4,'Waiting for task to start','2023-08-10T14:01:46.033224644Z');
INSERT INTO task_logs VALUES('340c6613-0e3f-487c-9dd0-88bf80145161',4,'Waiting for start','2023-08-10T14:02:43.231322216Z');
INSERT INTO task_logs VALUES('2622611e-ea2e-486f-9cb8-aa5a11f5959e',3,'Task started','2023-08-10T14:03:04.359216596Z');
INSERT INTO task_logs VALUES('2622611e-ea2e-486f-9cb8-aa5a11f5959e',3,'Started task for ingesting layers. Waiting for it to finish','2023-08-10T14:03:04.359935398Z');
INSERT INTO task_logs VALUES('2622611e-ea2e-486f-9cb8-aa5a11f5959e',0,'Failed to ingest layers','2023-08-10T14:03:04.360403608Z');
INSERT INTO task_logs VALUES('bf7ce4f4-0ff6-494b-b8d6-98087741f605',3,'Task started','2023-08-10T14:03:04.361966447Z');
INSERT INTO task_logs VALUES('bf7ce4f4-0ff6-494b-b8d6-98087741f605',3,'Starting download of image config','2023-08-10T14:03:04.362390466Z');
INSERT INTO task_logs VALUES('8940d412-8efd-4ea1-9458-8dfa87b9db2f',4,'Fetching and parsing config sha256:13baadde52d99b8e75ea2c9a2c4d21c5577e277c8acf30329dd1f359c0dd8d1c','2023-08-10T14:03:04.363418592Z');
INSERT INTO task_logs VALUES('aadfc6ae-0a03-4ce2-ade3-b0ed0edca0d1',3,'Started task','2023-08-10T14:03:04.365285487Z');
INSERT INTO task_logs VALUES('aadfc6ae-0a03-4ce2-ade3-b0ed0edca0d1',3,'Started creating chain, waiting for it to finish','2023-08-10T14:03:04.366914094Z');
INSERT INTO task_logs VALUES('a10190f9-bb2d-4aa8-9bfc-39e3d9eef831',3,'Started task','2023-08-10T14:03:04.368603672Z');
INSERT INTO task_logs VALUES('a10190f9-bb2d-4aa8-9bfc-39e3d9eef831',4,'Created .chains directory','2023-08-10T14:03:04.36994088Z');
INSERT INTO task_logs VALUES('a10190f9-bb2d-4aa8-9bfc-39e3d9eef831',3,'Started creating chain links, waiting for them to finish','2023-08-10T14:03:04.374201241Z');
INSERT INTO task_logs VALUES('672fc2e5-a644-4687-bcc5-932999b3b55e',3,'Started task','2023-08-10T14:03:04.375150893Z');
INSERT INTO task_logs VALUES('672fc2e5-a644-4687-bcc5-932999b3b55e',3,'Started task for downloading layer, waiting for it to finish','2023-08-10T14:03:04.376220324Z');
INSERT INTO task_logs VALUES('d10144c7-bb18-4f1c-aa55-b70f7fbd4429',3,'Started task','2023-08-10T14:03:04.377287268Z');
INSERT INTO task_logs VALUES('d10144c7-bb18-4f1c-aa55-b70f7fbd4429',3,'Started task for downloading layer, waiting for it to finish','2023-08-10T14:03:04.37820304Z');
INSERT INTO task_logs VALUES('aba07806-5074-4ae7-bcb4-46de49264437',3,'Started task','2023-08-10T14:03:04.379105406Z');
INSERT INTO task_logs VALUES('aba07806-5074-4ae7-bcb4-46de49264437',3,'Started task for downloading layer, waiting for it to finish','2023-08-10T14:03:04.379997168Z');
INSERT INTO task_logs VALUES('f2f2a8b7-dc43-407f-a258-5c49fa3d67b4',3,'Started task','2023-08-10T14:03:04.380933273Z');
INSERT INTO task_logs VALUES('f2f2a8b7-dc43-407f-a258-5c49fa3d67b4',3,'Started task for downloading layer, waiting for it to finish','2023-08-10T14:03:04.381316033Z');
INSERT INTO task_logs VALUES('c3fbb9fb-cba1-4f58-aa2e-ddc39a24ac5c',3,'Started task','2023-08-10T14:03:04.382197245Z');
INSERT INTO task_logs VALUES('c3fbb9fb-cba1-4f58-aa2e-ddc39a24ac5c',3,'Started task for downloading layer, waiting for it to finish','2023-08-10T14:03:04.382578973Z');
INSERT INTO task_logs VALUES('81ff1c6f-d157-4584-8dc4-6f4c6ecd52c2',4,'Downloading blob sha256:4f4fb700ef54461cfa02571ae0db9a0dc1e0cdb5577484a6d75e68dc38e8acc1','2023-08-10T14:03:04.383092203Z');
INSERT INTO task_logs VALUES('3eac4b1f-af2d-474e-ad86-42db941b2233',4,'Downloading blob sha256:ee94074a33e6da5eff1f3c5c00fb01515fdeffb3122380190eef77dc8481cd2a','2023-08-10T14:03:04.383582872Z');
INSERT INTO task_logs VALUES('348475ce-ec98-47b7-ac0b-02bf372375fe',4,'Downloading blob sha256:a4e38e86da02e5220fd86c59c09431c9220f7b00d6941ed958dfa4609cab9e44','2023-08-10T14:03:04.384423384Z');
INSERT INTO task_logs VALUES('c05d4410-25c3-4861-adce-3a4f654a409f',3,'Started task','2023-08-10T14:03:04.39020069Z');
INSERT INTO task_logs VALUES('c05d4410-25c3-4861-adce-3a4f654a409f',3,'Started task for downloading layer, waiting for it to finish','2023-08-10T14:03:04.391252325Z');
INSERT INTO task_logs VALUES('a31f6685-fa68-4ba2-a59d-7bb43cffe18b',4,'Downloading blob sha256:f9277b307cad05c20e944285815bf89c32dbbb7af33126e8bb828ca0aa80cd3b','2023-08-10T14:03:04.391295612Z');
INSERT INTO task_logs VALUES('b4a54c5f-db8d-4644-b157-220500e61793',3,'Started task','2023-08-10T14:03:04.406116439Z');
INSERT INTO task_logs VALUES('b4a54c5f-db8d-4644-b157-220500e61793',3,'Started task for downloading layer, waiting for it to finish','2023-08-10T14:03:04.407108217Z');
INSERT INTO task_logs VALUES('dc0ce7d3-9ba5-4002-bb5b-ed45bec555d1',4,'Downloading blob sha256:bedb029d66e037182750ce60cbd0805938bb9b28ccf4335f1f14059f93c75698','2023-08-10T14:03:04.407143352Z');
INSERT INTO task_logs VALUES('659dae44-6360-4e1f-ab7e-3dbcf0ed0437',3,'Started task','2023-08-10T14:03:04.426719248Z');
INSERT INTO task_logs VALUES('659dae44-6360-4e1f-ab7e-3dbcf0ed0437',3,'Started task for downloading layer, waiting for it to finish','2023-08-10T14:03:04.42770257Z');
INSERT INTO task_logs VALUES('62edacd1-2986-491e-a833-5f54bff18a1d',4,'Downloading blob sha256:061201b478a4fa581f9527d8293de4d6882e2cd0e493b798121a78c6416f1e6b','2023-08-10T14:03:04.427731144Z');
INSERT INTO task_logs VALUES('3eac4b1f-af2d-474e-ad86-42db941b2233',4,'Finished downloading blob sha256:ee94074a33e6da5eff1f3c5c00fb01515fdeffb3122380190eef77dc8481cd2a','2023-08-10T14:03:05.337810872Z');
INSERT INTO task_logs VALUES('d10144c7-bb18-4f1c-aa55-b70f7fbd4429',3,'Successfully downloaded layer','2023-08-10T14:03:05.33930295Z');
INSERT INTO task_logs VALUES('d10144c7-bb18-4f1c-aa55-b70f7fbd4429',3,'Waiting for ingestion of previous chain link (sha256:70843129a120de38cffbc279010d5bca3f7f00bad3873ad803e1345bda5c823d) to complete.','2023-08-10T14:03:05.339722961Z');
INSERT INTO task_logs VALUES('8940d412-8efd-4ea1-9458-8dfa87b9db2f',4,'Finished fetching and parsing config sha256:13baadde52d99b8e75ea2c9a2c4d21c5577e277c8acf30329dd1f359c0dd8d1c','2023-08-10T14:03:05.341331453Z');
INSERT INTO task_logs VALUES('bf7ce4f4-0ff6-494b-b8d6-98087741f605',3,'Successfully downloaded image config','2023-08-10T14:03:05.342607973Z');
INSERT INTO task_logs VALUES('bf7ce4f4-0ff6-494b-b8d6-98087741f605',4,'Starting CVMFS transaction','2023-08-10T14:03:05.343158806Z');
INSERT INTO task_logs VALUES('a31f6685-fa68-4ba2-a59d-7bb43cffe18b',4,'Finished downloading blob sha256:f9277b307cad05c20e944285815bf89c32dbbb7af33126e8bb828ca0aa80cd3b','2023-08-10T14:03:05.343649205Z');
INSERT INTO task_logs VALUES('c05d4410-25c3-4861-adce-3a4f654a409f',3,'Successfully downloaded layer','2023-08-10T14:03:05.345139095Z');
INSERT INTO task_logs VALUES('c05d4410-25c3-4861-adce-3a4f654a409f',3,'Waiting for ingestion of previous chain link (sha256:c5db43550e52c883e7dd82f05771cb1bcf68ba48a92b0f08864c05c891cf98cb) to complete.','2023-08-10T14:03:05.345734412Z');
INSERT INTO task_logs VALUES('c05d4410-25c3-4861-adce-3a4f654a409f',3,'Previous chain link successfully ingested','2023-08-10T14:03:05.34654077Z');
INSERT INTO task_logs VALUES('c05d4410-25c3-4861-adce-3a4f654a409f',3,'Started task for ingesting chain link, waiting for it to finish','2023-08-10T14:03:05.347739273Z');
INSERT INTO task_logs VALUES('b13938dd-1086-472c-959e-e9ee0cf4eda6',3,'Started task','2023-08-10T14:03:05.349728723Z');
INSERT INTO task_logs VALUES('b13938dd-1086-472c-959e-e9ee0cf4eda6',4,'Sleeping for 1 second to ensure that cvmfs is ready for new ingestion.','2023-08-10T14:03:05.350693323Z');
INSERT INTO task_logs VALUES('81ff1c6f-d157-4584-8dc4-6f4c6ecd52c2',4,'Finished downloading blob sha256:4f4fb700ef54461cfa02571ae0db9a0dc1e0cdb5577484a6d75e68dc38e8acc1','2023-08-10T14:03:05.350529163Z');
INSERT INTO task_logs VALUES('c3fbb9fb-cba1-4f58-aa2e-ddc39a24ac5c',3,'Successfully downloaded layer','2023-08-10T14:03:05.353042495Z');
INSERT INTO task_logs VALUES('c3fbb9fb-cba1-4f58-aa2e-ddc39a24ac5c',3,'Waiting for ingestion of previous chain link (sha256:4b347d63b24574f2eac082293d0aaff897aadbd363fee3d55780ca2c9d82ba86) to complete.','2023-08-10T14:03:05.353519256Z');
INSERT INTO task_logs VALUES('c3fbb9fb-cba1-4f58-aa2e-ddc39a24ac5c',0,'Previous chain link failed','2023-08-10T14:03:05.354044359Z');
INSERT INTO task_logs VALUES('a10190f9-bb2d-4aa8-9bfc-39e3d9eef831',0,'Failed to ingest all chain links','2023-08-10T14:03:05.355008649Z');
INSERT INTO task_logs VALUES('aadfc6ae-0a03-4ce2-ade3-b0ed0edca0d1',0,'Chain ingest failed','2023-08-10T14:03:05.3571467Z');
INSERT INTO task_logs VALUES('340c6613-0e3f-487c-9dd0-88bf80145161',4,'Starting podman image creation','2023-08-10T14:03:05.358483271Z');
INSERT INTO task_logs VALUES('cf1220ef-2cc5-4c9a-badb-c69e6799f633',4,'Fetching and parsing config sha256:13baadde52d99b8e75ea2c9a2c4d21c5577e277c8acf30329dd1f359c0dd8d1c','2023-08-10T14:03:05.359344154Z');
INSERT INTO task_logs VALUES('348475ce-ec98-47b7-ac0b-02bf372375fe',4,'Finished downloading blob sha256:a4e38e86da02e5220fd86c59c09431c9220f7b00d6941ed958dfa4609cab9e44','2023-08-10T14:03:05.352254745Z');
INSERT INTO task_logs VALUES('aba07806-5074-4ae7-bcb4-46de49264437',3,'Successfully downloaded layer','2023-08-10T14:03:05.361550448Z');
INSERT INTO task_logs VALUES('aba07806-5074-4ae7-bcb4-46de49264437',3,'Waiting for ingestion of previous chain link (sha256:8db5443fbaea4961298243b6b669f63d0ed20b8cb40979edc777c489f018a335) to complete.','2023-08-10T14:03:05.362096184Z');
INSERT INTO task_logs VALUES('f2f2a8b7-dc43-407f-a258-5c49fa3d67b4',3,'Successfully downloaded layer','2023-08-10T14:03:05.363929052Z');
INSERT INTO task_logs VALUES('f2f2a8b7-dc43-407f-a258-5c49fa3d67b4',3,'Waiting for ingestion of previous chain link (sha256:34e027fafa03d426cbffd4594a9f6da5d5679e90276c8d6ae205f277c1fa00b9) to complete.','2023-08-10T14:03:05.364609159Z');
INSERT INTO task_logs VALUES('dc0ce7d3-9ba5-4002-bb5b-ed45bec555d1',4,'Finished downloading blob sha256:bedb029d66e037182750ce60cbd0805938bb9b28ccf4335f1f14059f93c75698','2023-08-10T14:03:05.369461516Z');
INSERT INTO task_logs VALUES('b4a54c5f-db8d-4644-b157-220500e61793',3,'Successfully downloaded layer','2023-08-10T14:03:05.370582138Z');
INSERT INTO task_logs VALUES('b4a54c5f-db8d-4644-b157-220500e61793',3,'Waiting for ingestion of previous chain link (sha256:6ff52e38ae5f616cf2db193d9024dbd64af751d3f68acd3f303ef7aa41635c5c) to complete.','2023-08-10T14:03:05.371261233Z');
INSERT INTO task_logs VALUES('672fc2e5-a644-4687-bcc5-932999b3b55e',3,'Successfully downloaded layer','2023-08-10T14:03:05.355734318Z');
INSERT INTO task_logs VALUES('672fc2e5-a644-4687-bcc5-932999b3b55e',3,'Waiting for ingestion of previous chain link (sha256:1eb307c554961b7aa4c2244ab7bca12f7016067d27342251bb2785627cc6e55e) to complete.','2023-08-10T14:03:05.374687529Z');
INSERT INTO task_logs VALUES('62edacd1-2986-491e-a833-5f54bff18a1d',4,'Finished downloading blob sha256:061201b478a4fa581f9527d8293de4d6882e2cd0e493b798121a78c6416f1e6b','2023-08-10T14:03:05.390152266Z');
INSERT INTO task_logs VALUES('659dae44-6360-4e1f-ab7e-3dbcf0ed0437',3,'Successfully downloaded layer','2023-08-10T14:03:05.391362474Z');
INSERT INTO task_logs VALUES('659dae44-6360-4e1f-ab7e-3dbcf0ed0437',3,'Waiting for ingestion of previous chain link (sha256:2d5500194e44a96c3fc2d6b75bf96429f19e002d386170bcb4a87c9f285077a4) to complete.','2023-08-10T14:03:05.391868038Z');
INSERT INTO task_logs VALUES('bf7ce4f4-0ff6-494b-b8d6-98087741f605',0,'CvmFS transaction failed: exit status 17','2023-08-10T14:03:05.58821287Z');
INSERT INTO task_logs VALUES('cf1220ef-2cc5-4c9a-badb-c69e6799f633',4,'Finished fetching and parsing config sha256:13baadde52d99b8e75ea2c9a2c4d21c5577e277c8acf30329dd1f359c0dd8d1c','2023-08-10T14:03:05.71553193Z');
INSERT INTO task_logs VALUES('340c6613-0e3f-487c-9dd0-88bf80145161',3,'Successfully created Podman metadata catalogs','2023-08-10T14:03:05.721502928Z');
INSERT INTO task_logs VALUES('340c6613-0e3f-487c-9dd0-88bf80145161',3,'Getting existing image metadata','2023-08-10T14:03:05.722895145Z');
INSERT INTO task_logs VALUES('340c6613-0e3f-487c-9dd0-88bf80145161',3,'Successfully got existing image metadata','2023-08-10T14:03:05.723971875Z');
INSERT INTO task_logs VALUES('340c6613-0e3f-487c-9dd0-88bf80145161',3,'Podman image already exists in cvmfs, and metadata is up to date. Skipping creation.','2023-08-10T14:03:05.724778121Z');
CREATE TABLE IF NOT EXISTS "task_relations" (
    task1_id TEXT NOT NULL,
    task2_id TEXT NOT NULL,
    relation TEXT NOT NULL,

    PRIMARY KEY (task1_id, task2_id, relation),
    FOREIGN KEY (task1_id) REFERENCES tasks (id) ON DELETE CASCADE,
    FOREIGN KEY (task2_id) REFERENCES tasks (id) ON DELETE CASCADE
);
INSERT INTO task_relations VALUES('8b1a9925-ccd3-4a25-92ab-6cbfda77ee9c','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('d19e4bb8-1a4f-49fc-9b9a-909e38b9e911','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('143d5cb6-fcbd-4a7d-be11-0f967cb9235b','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('2258ba27-230d-47fc-988b-e3fab0d5b62f','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('56600328-3a61-432e-a33e-c7747d486b8a','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('a31f6685-fa68-4ba2-a59d-7bb43cffe18b','c05d4410-25c3-4861-adce-3a4f654a409f','SUBTASK_OF');
INSERT INTO task_relations VALUES('b13938dd-1086-472c-959e-e9ee0cf4eda6','c05d4410-25c3-4861-adce-3a4f654a409f','SUBTASK_OF');
INSERT INTO task_relations VALUES('c05d4410-25c3-4861-adce-3a4f654a409f','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('dc0ce7d3-9ba5-4002-bb5b-ed45bec555d1','b4a54c5f-db8d-4644-b157-220500e61793','SUBTASK_OF');
INSERT INTO task_relations VALUES('f2d95f3b-f5d6-4449-ae82-a0fba60d9a6b','b4a54c5f-db8d-4644-b157-220500e61793','SUBTASK_OF');
INSERT INTO task_relations VALUES('b4a54c5f-db8d-4644-b157-220500e61793','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('62edacd1-2986-491e-a833-5f54bff18a1d','659dae44-6360-4e1f-ab7e-3dbcf0ed0437','SUBTASK_OF');
INSERT INTO task_relations VALUES('d71f242b-07f7-4856-88a3-9be4debbca6b','659dae44-6360-4e1f-ab7e-3dbcf0ed0437','SUBTASK_OF');
INSERT INTO task_relations VALUES('659dae44-6360-4e1f-ab7e-3dbcf0ed0437','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('81ff1c6f-d157-4584-8dc4-6f4c6ecd52c2','672fc2e5-a644-4687-bcc5-932999b3b55e','SUBTASK_OF');
INSERT INTO task_relations VALUES('e1bce77e-aeb2-4dfa-886a-b73f7aeb3041','672fc2e5-a644-4687-bcc5-932999b3b55e','SUBTASK_OF');
INSERT INTO task_relations VALUES('672fc2e5-a644-4687-bcc5-932999b3b55e','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('3eac4b1f-af2d-474e-ad86-42db941b2233','d10144c7-bb18-4f1c-aa55-b70f7fbd4429','SUBTASK_OF');
INSERT INTO task_relations VALUES('7dcc2563-3b1b-4a68-a96d-aefbbd23f218','d10144c7-bb18-4f1c-aa55-b70f7fbd4429','SUBTASK_OF');
INSERT INTO task_relations VALUES('d10144c7-bb18-4f1c-aa55-b70f7fbd4429','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('348475ce-ec98-47b7-ac0b-02bf372375fe','aba07806-5074-4ae7-bcb4-46de49264437','SUBTASK_OF');
INSERT INTO task_relations VALUES('75d40437-d196-493e-bbc4-005822d9dc56','aba07806-5074-4ae7-bcb4-46de49264437','SUBTASK_OF');
INSERT INTO task_relations VALUES('aba07806-5074-4ae7-bcb4-46de49264437','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('81ff1c6f-d157-4584-8dc4-6f4c6ecd52c2','f2f2a8b7-dc43-407f-a258-5c49fa3d67b4','SUBTASK_OF');
INSERT INTO task_relations VALUES('ee7314b5-8bf0-40e4-a77e-ae38b3fef3c9','f2f2a8b7-dc43-407f-a258-5c49fa3d67b4','SUBTASK_OF');
INSERT INTO task_relations VALUES('f2f2a8b7-dc43-407f-a258-5c49fa3d67b4','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('9029275f-9fc0-4a85-b0a9-c7e976420392','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('42978afd-8016-4354-8918-ca354512ae6f','38b75513-c972-470a-8bcc-fd2b81690269','SUBTASK_OF');
INSERT INTO task_relations VALUES('38b75513-c972-470a-8bcc-fd2b81690269','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('81ff1c6f-d157-4584-8dc4-6f4c6ecd52c2','c3fbb9fb-cba1-4f58-aa2e-ddc39a24ac5c','SUBTASK_OF');
INSERT INTO task_relations VALUES('8e3e106d-9b0f-4d13-a469-f6c50b7a00e9','c3fbb9fb-cba1-4f58-aa2e-ddc39a24ac5c','SUBTASK_OF');
INSERT INTO task_relations VALUES('c3fbb9fb-cba1-4f58-aa2e-ddc39a24ac5c','a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','SUBTASK_OF');
INSERT INTO task_relations VALUES('a10190f9-bb2d-4aa8-9bfc-39e3d9eef831','aadfc6ae-0a03-4ce2-ade3-b0ed0edca0d1','SUBTASK_OF');
INSERT INTO task_relations VALUES('904132d9-9a2f-491d-96ca-b68370ceef15','aadfc6ae-0a03-4ce2-ade3-b0ed0edca0d1','SUBTASK_OF');
INSERT INTO task_relations VALUES('61b69ba8-caf7-4364-a1b9-6310129dd598','5f65deb4-3330-432e-bd25-20134d081aad','SUBTASK_OF');
INSERT INTO task_relations VALUES('81ff1c6f-d157-4584-8dc4-6f4c6ecd52c2','94ede230-66e2-412f-a934-64f8d2ea4cbd','SUBTASK_OF');
INSERT INTO task_relations VALUES('dddf892f-146e-4b4f-ad81-ee612669eb31','94ede230-66e2-412f-a934-64f8d2ea4cbd','SUBTASK_OF');
INSERT INTO task_relations VALUES('94ede230-66e2-412f-a934-64f8d2ea4cbd','5f65deb4-3330-432e-bd25-20134d081aad','SUBTASK_OF');
INSERT INTO task_relations VALUES('669aadaa-12ec-493a-a4af-aeef30dedb6a','92f42c68-9139-4ec3-9d72-ecf1a68cb55f','SUBTASK_OF');
INSERT INTO task_relations VALUES('0ee87f2b-b79d-45fe-a0b2-68b9e238ca53','92f42c68-9139-4ec3-9d72-ecf1a68cb55f','SUBTASK_OF');
INSERT INTO task_relations VALUES('92f42c68-9139-4ec3-9d72-ecf1a68cb55f','5f65deb4-3330-432e-bd25-20134d081aad','SUBTASK_OF');
INSERT INTO task_relations VALUES('62f8edda-78b4-4fc8-b8a5-ebd14c3220a3','dcae3c2e-e984-476b-8533-3674b2e85600','SUBTASK_OF');
INSERT INTO task_relations VALUES('28a86c2d-e2d2-4e69-a555-acfe4d7f8263','dcae3c2e-e984-476b-8533-3674b2e85600','SUBTASK_OF');
INSERT INTO task_relations VALUES('5f65deb4-3330-432e-bd25-20134d081aad','2622611e-ea2e-486f-9cb8-aa5a11f5959e','SUBTASK_OF');
INSERT INTO task_relations VALUES('8940d412-8efd-4ea1-9458-8dfa87b9db2f','bf7ce4f4-0ff6-494b-b8d6-98087741f605','SUBTASK_OF');
INSERT INTO task_relations VALUES('bf7ce4f4-0ff6-494b-b8d6-98087741f605','2622611e-ea2e-486f-9cb8-aa5a11f5959e','SUBTASK_OF');
INSERT INTO task_relations VALUES('cf1220ef-2cc5-4c9a-badb-c69e6799f633','340c6613-0e3f-487c-9dd0-88bf80145161','SUBTASK_OF');
CREATE INDEX "wishes_source_idx" ON "wishes" ("source");
COMMIT;
