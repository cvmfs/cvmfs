from __future__ import print_function
import unittest
import cvmfs_globals
cvmfs_globals.CVMFS_UNITTESTS = True

import cvmfs_geo
from cvmfs_geo import distance_on_unit_sphere

class GeoTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testDistance(self):
        self.assertEqual(0.0, distance_on_unit_sphere(0, 0, 0, 0))

if __name__ == '__main__':
    unittest.main()
