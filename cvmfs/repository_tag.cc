/**
 * This file is part of the CernVM File System.
 */

#include "repository_tag.h"

RepositoryTag::RepositoryTag(const std::string& name,
                             const std::string& channel,
                             const std::string& description)
    : name_(name),
      channel_(channel),
      description_(description) {
}