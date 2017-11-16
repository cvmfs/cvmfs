/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_REPOSITORY_TAG_H_
#define CVMFS_REPOSITORY_TAG_H_

#include <string>

struct RepositoryTag {
    RepositoryTag() : name_(""), channel_(""), description_("") {}

    RepositoryTag(const std::string& name,
                  const std::string& channel,
                  const std::string& description);
            
    std::string name_;
    std::string channel_;
    std::string description_;
};
 
#endif  // CVMFS_REPOSITORY_TAG_H_