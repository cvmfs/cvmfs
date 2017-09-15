from __future__ import print_function
import unittest
import socket
import time
import cvmfs_globals
cvmfs_globals.CVMFS_UNITTESTS = True

import cvmfs_geo
from cvmfs_geo import distance_on_unit_sphere
from cvmfs_geo import addr_geoinfo
from cvmfs_geo import name_geoinfo

# Simulate a small geo IP database, since we can't always
#  expect a full one to be available.  IPv4 addresses are always
#  preferred, so for those with IPv6 use only IPv6.

def getaddrs(name, type):
    addrs = []
    for info in socket.getaddrinfo(name,80,0,0,socket.IPPROTO_TCP):
        if info[0] == type:
            addrs.append(info[4][0])
    return addrs

CERNgeo = {
    'latitude': 46.2324,
    'longitude': 6.0502
}
CERNname = 'cvmfs-stratum-one.cern.ch'
CERNv6addrs = getaddrs(CERNname, socket.AF_INET6)
FNALgeo = {
    'latitude': 41.7768,
    'longitude': -88.4604
}
FNALname = 'cvmfs.fnal.gov'
FNALv4addrs = getaddrs(FNALname, socket.AF_INET)
IHEPgeo = {
    'latitude': 39.9289,
    'longitude': 116.3883
}
IHEPname = 'cvmfs-stratum-one.ihep.ac.cn'
IHEPv4addrs = getaddrs(IHEPname, socket.AF_INET)
RALgeo = {
    'latitude': 51.75,
    'longitude': -1.25
}
RALname = 'cernvmfs.gridpp.rl.ac.uk'
RALv6addrs = getaddrs(RALname, socket.AF_INET6)

class giIPv4TestDb():
    def record_by_addr(self, addr):
        if addr in FNALv4addrs:
            return FNALgeo
        if addr in IHEPv4addrs:
            return IHEPgeo
        return None

class giIPv6TestDb():
    def record_by_addr_v6(self, addr):
        if addr in CERNv6addrs:
            return CERNgeo
        if addr in RALv6addrs:
            return RALgeo
        return None

cvmfs_geo.gi = giIPv4TestDb()
cvmfs_geo.gi6 = giIPv6TestDb()

class GeoTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testDistance(self):
        self.assertEqual(0.0, distance_on_unit_sphere(0, 0, 0, 0))
        self.assertAlmostEqual(1.11458455,
            distance_on_unit_sphere(FNALgeo['latitude'], FNALgeo['longitude'],
                                    CERNgeo['latitude'], CERNgeo['longitude']))
        self.assertAlmostEqual(1.11458455,
            distance_on_unit_sphere(CERNgeo['latitude'], CERNgeo['longitude'],
                                    FNALgeo['latitude'], FNALgeo['longitude']))
        self.assertAlmostEqual(1.6622382,
            distance_on_unit_sphere(IHEPgeo['latitude'], IHEPgeo['longitude'],
                                    FNALgeo['latitude'], FNALgeo['longitude']))
        self.assertAlmostEqual(0.1274021,
            distance_on_unit_sphere(CERNgeo['latitude'], CERNgeo['longitude'],
                                    RALgeo['latitude'],  RALgeo['longitude']))

    def testAddrGeoinfo(self):
        self.assertEqual(CERNgeo, addr_geoinfo(CERNv6addrs[0]))
        self.assertEqual(FNALgeo, addr_geoinfo(FNALv4addrs[0]))
        self.assertEqual(IHEPgeo, addr_geoinfo(IHEPv4addrs[0]))
        self.assertEqual(RALgeo,  addr_geoinfo(RALv6addrs[0]))

    def testNameGeoinfo(self):
        self.assertEqual(0, len(cvmfs_geo.geo_cache))
        now = 0
        self.assertEqual(CERNgeo, name_geoinfo(now, CERNname))
        self.assertEqual(FNALgeo, name_geoinfo(now, FNALname))
        self.assertEqual(IHEPgeo, name_geoinfo(now, IHEPname))
        self.assertEqual(RALgeo,  name_geoinfo(now, RALname))
        self.assertEqual(4, len(cvmfs_geo.geo_cache))

        # test the caching, when there's no database available
        savegi = cvmfs_geo.gi
        savegi6 = cvmfs_geo.gi6
        cvmfs_geo.gi = None
        cvmfs_geo.gi6 = None
        now = 1
        self.assertEqual(CERNgeo, name_geoinfo(now, CERNname))
        self.assertEqual(FNALgeo, name_geoinfo(now, FNALname))
        self.assertEqual(IHEPgeo, name_geoinfo(now, IHEPname))
        self.assertEqual(RALgeo,  name_geoinfo(now, RALname))
        cvmfs_geo.gi = savegi
        cvmfs_geo.gi6 = savegi6

if __name__ == '__main__':
    unittest.main()
