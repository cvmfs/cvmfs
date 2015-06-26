#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import math
from datetime    import datetime
from dateutil.tz import tzutc

from _common import _logistic_function

class Availability:
    def __init__(self, stratum0, **kwargs):
        self.stratum0  = stratum0
        self.stratum1s = []
        self._get_param_or_default('revision_threshold',     5, **kwargs)
        self._get_param_or_default('replication_threshold', 60, **kwargs)

    def _get_param_or_default(self, argname, default, **kwargs):
        if argname not in kwargs:
            return default
        else:
            setattr(self, argname, kwargs[argname])

    def add_stratum1(self, stratum1):
        self.stratum1s.append(stratum1)

    def has_stratum1s(self):
        return len(self.stratum1s) > 0

    def get_current_revision(self):
        return self.stratum0.manifest.revision

    def get_oldest_revision(self):
        if self.has_stratum1s():
            return min([ s1.manifest.revision for s1 in self.stratum1s ])
        else:
            return self.get_current_revision()

    def get_oldest_stratum1(self):
        if self.has_stratum1s():
            oldest_s1 = None
            revision  = self.get_current_revision()
            for s1 in self.stratum1s:
                if revision > s1.manifest.revision:
                    oldest_s1 = s1
                    revision  = s1.manifest.revision
            return oldest_s1
        else:
            return None

    def get_stratum1_health_score(self, stratum1):
        f_revision    = _logistic_function(self.revision_threshold)
        f_replication = _logistic_function(self.replication_threshold)
        d_revision    = self.get_current_revision() - stratum1.manifest.revision
        d_replication = datetime.now(tzutc()) - stratum1.last_replication
        d_replication = d_replication.days * 24*60*60 + d_replication.seconds
        return f_revision(d_revision) * f_replication(d_replication / 60.0)

    def get_stratum1_health_scores(self):
        get_score = lambda s1: self.get_stratum1_health_score(s1)
        return [ (s1, get_score(s1)) for s1 in self.stratum1s ]

    def get_repository_health_score(self):
        scores = [ score for s1, score in self.get_stratum1_health_scores() ]
        return reduce(lambda x,y: x*y, scores)

