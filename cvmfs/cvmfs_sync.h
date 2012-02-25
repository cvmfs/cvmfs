#ifndef CVMFS_SYNC_H
#define CVMFS_SYNC_H 1

typedef struct {
	std::string dir_shadow;
	std::string dir_data;
	std::string dir_catalogs;
	std::string dir_cvmfs;
	std::string dir_overlay;
	std::string keyfile;
	std::set<std::string> immutables;
	bool print_changeset;
	bool dry_run;
	bool lazy_attach;
	int sync_threads;
	bool mucatalogs;
} SyncParameters;

#endif
