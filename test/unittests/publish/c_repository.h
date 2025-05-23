/**
 * This file is part of the CernVM File System.
 */

#ifndef TEST_UNITTESTS_PUBLISH_C_REPOSITORY_H_
#define TEST_UNITTESTS_PUBLISH_C_REPOSITORY_H_

#include "publish/repository.h"

publish::Publisher *GetTestPublisher();
publish::Repository *GetRepositoryFromPublisher(
    const publish::Publisher &publisher);

#endif  // TEST_UNITTESTS_PUBLISH_C_REPOSITORY_H_
