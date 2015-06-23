#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import ctypes
import tempfile
import zlib
import sqlite3
import subprocess
import shutil
import math


_REPO_CONFIG_PATH      = "/etc/cvmfs/repositories.d"
_SERVER_CONFIG_NAME    = "server.conf"

_MANIFEST_NAME         = ".cvmfspublished"
_WHITELIST_NAME        = ".cvmfswhitelist"
_LAST_REPLICATION_NAME = ".cvmfs_last_snapshot"
_REPLICATING_NAME      = ".cvmfs_is_snapshotting"


class CvmfsNotInstalled(Exception):
    def __init__(self):
        Exception.__init__(self, "It seems that cvmfs is not installed on this machine!")


class CompressedObject:
    file_            = None
    compressed_file_ = None

    def __init__(self, compressed_file):
        self.compressed_file_ = compressed_file
        self._decompress()

    def get_compressed_file(self):
        return self.compressed_file_

    def get_uncompressed_file(self):
        return self.file_

    def save_to(self, path):
        shutil.copyfile(self.get_compressed_file().name, path)

    def save_uncompressed_to(self, path):
        shutil.copyfile(self.get_uncompressed_file().name, path)

    def _decompress(self):
        """ Unzip a file to a temporary referenced by self.file_ """
        self.file_ = tempfile.NamedTemporaryFile('w+b')
        self.compressed_file_.seek(0)
        self.file_.write(zlib.decompress(self.compressed_file_.read()))
        self.file_.flush()
        self.file_.seek(0)
        self.compressed_file_.seek(0)

    def _close(self):
        if self.file_:
            self.file_.close()
        if self.compressed_file_:
            self.compressed_file_.close()


class FileObject(CompressedObject):
    def __init__(self, compressed_file):
        CompressedObject.__init__(self, compressed_file)

    def file(self):
        return self.get_uncompressed_file()


def _logistic_function(a):
    return lambda x: round(1 - (1/(1 + math.exp(-5.5 * ((float(x)/float(a)) - 1)))), 2)


class TzInfos:
    tzd = None

    @staticmethod
    def get_tzinfos():
        """ Time Zone Codes are ambiguous but dateutil.parser.parse allows to
            pass a desired mapping of these codes to offsets to UTC.

            This is taken from Stack Overflow:
            http://stackoverflow.com/questions/1703546/
            parsing-date-time-string-with-timezone-abbreviated-name-in-python/
            4766400#4766400
        """
        if not TzInfos.tzd:
            TzInfos._generate_tzd()
        return TzInfos.tzd


    @staticmethod
    def _generate_tzd():
        print "generating"
        TzInfos.tzd = {}
        tz_str = '''-12 Y
-11 X NUT SST
-10 W CKT HAST HST TAHT TKT
-9 V AKST GAMT GIT HADT HNY
-8 U AKDT CIST HAY HNP PST PT
-7 T HAP HNR MST PDT
-6 S CST EAST GALT HAR HNC MDT
-5 R CDT COT EASST ECT EST ET HAC HNE PET
-4 Q AST BOT CLT COST EDT FKT GYT HAE HNA PYT
-3 P ADT ART BRT CLST FKST GFT HAA PMST PYST SRT UYT WGT
-2 O BRST FNT PMDT UYST WGST
-1 N AZOT CVT EGT
0 Z EGST GMT UTC WET WT
1 A CET DFT WAT WEDT WEST
2 B CAT CEDT CEST EET SAST WAST
3 C EAT EEDT EEST IDT MSK
4 D AMT AZT GET GST KUYT MSD MUT RET SAMT SCT
5 E AMST AQTT AZST HMT MAWT MVT PKT TFT TJT TMT UZT YEKT
6 F ALMT BIOT BTT IOT KGT NOVT OMST YEKST
7 G CXT DAVT HOVT ICT KRAT NOVST OMSST THA WIB
8 H ACT AWST BDT BNT CAST HKT IRKT KRAST MYT PHT SGT ULAT WITA WST
9 I AWDT IRKST JST KST PWT TLT WDT WIT YAKT
10 K AEST ChST PGT VLAT YAKST YAPT
11 L AEDT LHDT MAGT NCT PONT SBT VLAST VUT
12 M ANAST ANAT FJT GILT MAGST MHT NZST PETST PETT TVT WFT
13 FJST NZDT
11.5 NFT
10.5 ACDT LHST
9.5 ACST
6.5 CCT MMT
5.75 NPT
5.5 SLT
4.5 AFT IRDT
3.5 IRST
-2.5 HAT NDT
-3.5 HNT NST NT
-4.5 HLV VET
-9.5 MART MIT'''

        for tz_descr in map(str.split, tz_str.split('\n')):
            tz_offset = int(float(tz_descr[0]) * 3600)
            for tz_code in tz_descr[1:]:
                TzInfos.tzd[tz_code] = tz_offset
