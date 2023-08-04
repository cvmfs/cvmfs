
## Types of tasks
- ExpandWildcard
- GetManifest

- IngestLayers
- IngestLayer

- IngestChain (might not be a task)
- IngestChainLink

- CreateFlat
- CreateImage

- DownloadBlob
- 


# Rules!
Any task can be aborted at any time and still leave the system in a consistent state.
Some cleanup might be needed. This must be performed during startup.


# User actions
- Sync Image:
    - ExpandWildcard
    - GetManifest
    - Update
- Sync Wish:
    - ExpandWildcard
    - GetManifest for each image
    - Update each image


## ExpandWildcard
### Triggers
- When the wish is created
- When the wish hasn't been expanded for {EXPAND_WILDCARD_TIMEOUT} seconds
- When spesifically requested by the user
### After
- Schedule update for any new images


## GetManifest
### Triggers
- When the layer is created
- When the manifest hasn't been fetched for {FETCH_MANIFEST_TIMEOUT} seconds
- When spesifically requested by the user
### Wait for
- ExpandWildcard for the same wish



# Stories
## User triggers wish sync
- Queue ExpandWildcard for the wish
- Queue all existing images for update


CREATE_IMAGE
  ingest_layers
    INGEST_LAYER
      download_blob
      - - -
      cvmfs_ingest_layer
      - - -
      write_layer_metadata
  - - - 
  write_image_metadata

CREATE_FLAT
  ingest_chain
    INGEST_CHAIN_LINK
      download_blob
      - - -
      cvmfs_ingest_chain_link
      - - -
      write_chain_link_metadata
  write_flat_metadata

CREATE_PODMAN
  
