#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import cvmfs

class WrongRepositoryType(Exception):
    def __init__(self, repo, expected_type):
        assert repo.type != expected_type
        self.repo          = repo
        self.expected_type = expected_type

    def __str__(self):
        return self.repo.fqrn + " is of type '" + self.repo.type  + "' but '" + self.expected_type + "' was expected"


class AvailabilityAssessment:
    def _check_repo_type(self, repo, expected_type):
        if repo.has_repository_type() and repo.type != expected_type:
            raise WrongRepositoryType(repo, expected_type)
        return True;

    def __init__(self, stratum0_repository, stratum1_repositories = []):
        self._check_repo_type(stratum0_repository, 'stratum0')
        for stratum1 in stratum1_repositories:
            self._check_repo_type(stratum1, 'stratum1')
        self.stratum0  = stratum0_repository
        self.stratum1s = stratum1_repositories

    def assess(self):
        pass
