#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

from datetime    import datetime, timedelta
from dateutil.tz import tzutc

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import cvmfs

class MockManifest:
    def __init__(self, **kwargs):
        for kw, arg in kwargs.iteritems():
            setattr(self, kw, arg)

class MockRepository:
    def __init__(self, **kwargs):
        self._filter_kwarg('last_replication', **kwargs)
        self.manifest = MockManifest(**kwargs)

    def _filter_kwarg(self, argname, **kwargs):
        if argname in kwargs:
            setattr(self, argname, kwargs[argname])
            del kwargs[argname]


class TestAvailability(unittest.TestCase):
    def _get_recent_time(self, minutes_ago):
        return datetime.now(tzutc()) - timedelta(seconds=minutes_ago*60)

    def test_get_oldest_revision(self):
        s0    = MockRepository(revision = 1000)
        avail = cvmfs.Availability(s0)
        self.assertEqual(1000, avail.get_oldest_revision())

        self.assertFalse(avail.has_stratum1s())
        avail.add_stratum1(MockRepository(revision = 1000))
        avail.add_stratum1(MockRepository(revision =  999))
        avail.add_stratum1(MockRepository(revision =  995))
        self.assertTrue(avail.has_stratum1s())
        self.assertEqual(995, avail.get_oldest_revision())


    def test_get_oldest_stratum1(self):
        s0    = MockRepository(revision = 1000)
        avail = cvmfs.Availability(s0)
        self.assertEqual(None, avail.get_oldest_stratum1())

        self.assertFalse(avail.has_stratum1s())
        avail.add_stratum1(MockRepository(revision = 1000))
        avail.add_stratum1(MockRepository(revision =  999))
        self.assertTrue(avail.has_stratum1s())

        oldest_s1 = MockRepository(revision =  995)
        avail.add_stratum1(oldest_s1)
        self.assertEqual(oldest_s1, avail.get_oldest_stratum1())


    def test_stratum1_scores(self):
        s0    = MockRepository(revision = 1000)
        avail = cvmfs.Availability(s0, revision_threshold=10, replication_threshold=60)
        self.assertEqual(1000, avail.get_oldest_revision())

        healthy  = MockRepository(revision = 1000, last_replication=self._get_recent_time(10))
        degraded = MockRepository(revision =  995, last_replication=self._get_recent_time(30))
        severe   = MockRepository(revision =  992, last_replication=self._get_recent_time(45))
        fatal    = MockRepository(revision =  980, last_replication=self._get_recent_time(90))

        self.assertLess(0.95, avail.get_stratum1_health_score(healthy))

        self.assertGreater(0.9, avail.get_stratum1_health_score(degraded))
        self.assertLess   (0.7, avail.get_stratum1_health_score(degraded))

        self.assertGreater(0.7, avail.get_stratum1_health_score(severe))
        self.assertLess   (0.4, avail.get_stratum1_health_score(severe))

        self.assertGreater(0.2, avail.get_stratum1_health_score(fatal))


    def test_repository_scores(self):
        s0    = MockRepository(revision = 1000)
        avail = cvmfs.Availability(s0, revision_threshold=10, replication_threshold=60)

        healthy  = MockRepository(revision = 1000, last_replication=self._get_recent_time(10))
        degraded = MockRepository(revision =  995, last_replication=self._get_recent_time(30))
        severe   = MockRepository(revision =  992, last_replication=self._get_recent_time(45))
        fatal    = MockRepository(revision =  980, last_replication=self._get_recent_time(90))

        avail.add_stratum1(healthy)
        avail.add_stratum1(healthy)
        self.assertLess(0.95, avail.get_repository_health_score())

        avail.add_stratum1(degraded)
        self.assertGreater(0.9, avail.get_repository_health_score())
        self.assertLess   (0.8, avail.get_repository_health_score())

        avail.add_stratum1(severe)
        self.assertGreater(0.8, avail.get_repository_health_score())
        self.assertLess   (0.5, avail.get_repository_health_score())

        avail.add_stratum1(severe)
        self.assertGreater(0.5, avail.get_repository_health_score())
        self.assertLess   (0.2, avail.get_repository_health_score())

        avail.add_stratum1(fatal)
        self.assertGreater(0.2, avail.get_repository_health_score())
