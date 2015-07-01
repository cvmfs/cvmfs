#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import base64
import unittest

from M2Crypto.X509 import X509

from file_sandbox import FileSandbox
import cvmfs

class TestCertificate(unittest.TestCase):
    def setUp(self):
        self.sandbox = FileSandbox("py_certificate_ut_")

        self.compressed_certificate = '\n'.join([
            'eJyVlDmvq0gQhXN+xeTWlTFgbAcTNHSDm53LYkPWYNYGA+ay+deP3wtGI71oKqjgVEmfjo6qvr4+',
            'JSEVW3/J6NvHCpaBj36JX4yJsXysZRmkgiy78kgWxfaqXTe/gSUVdChppV4WVgJuoAAIDqZvLqYb',
            'wdB1Idr6kSGqVRmt1aStMEVcUbhcuCVNbBlB7GPUBFjRmkQNy5QLCv+mTNHt0aRPc7mWqWX6eGE+',
            'jTX9gLWgu91+ix+Cj9//avWf1P9Cmf9DLQpUmYBVZW9QPZzw0EUS8/EGgIAluIBfCzro8Mcv1DG5',
            'uXXiJVvch6UVmncOxjsx4OeXezj2km91Lkv2YcoUwDQe2dPO63GXs7XArfSeT2ZrZwUeW0OwrmvG',
            'Cq1A+zDv+kgO2h88H0YjvOT+t/0omYYGr1NYNbjn0+aGnrNRBJG2hGzpPuMyL+M+u+3oco6+UzGb',
            'XDF5y/OmhLsmx4kjT4SJyDRSEiihfs/ZJldeb5PuzcvEvqqATGtipEPGnxpOQa8mLwih97dGLFGU',
            'dE+rj+6ZUQV5HDa1mO7aYb2AWSHTZrSLnIshD/1OPJXyXCx5aXeZjYxj1DpUIXUa45Mz4mSGjCEJ',
            'J4dvB/Tuif/TddkpihbT7oVQuHlt1qw2T4f27noacEBhSgCgBbqRpncxLufU+qSAFMkF8JPAZ0hh',
            'BEt6PmAhFbrxGImb+mBn9nVRLn4pWJofPvQDUkA7vjxqcFH0ZHRnuMv4hy6sthzDV3+lQ+yRya+v',
            'KtKFbLlVMKmMvX47bHsL9aXonh1pvTzaM/LsTZKYkWtIALOd1xVrmL/pdztCObiofbB+n8ZZJZJu',
            'U5LzY7ubDUrKmMh+dTKhtM116awDgzK0r9flshqOJGK450+CrBaOgPLJJnl/BZFs7nX9B8WHlK20',
            'Kz3UaU15XjPJ3LaKz+RWdoqV6bArinRP+vfDXVulyiLzrKRJbIFr5ltSfufKAa0XuCufaRFkhnHO',
            'Qqo/NjQysr/VLo2Hl7M3mjmcF9torwQAEu9hQW8iNwZ/M78vH1nwz2/wD2fJXIA=',
            ''
        ])
        compressed_cert = base64.b64decode(self.compressed_certificate)
        self.certificate_file = self.sandbox.write_to_temporary(compressed_cert)
        self.fingerprint = "81:8D:BE:49:A7:3E:F1:C4:DA:01:50:F0:80:D4:CB:27:96:80:1D"

        self.message_digest = "e380c3276b33440dd3e80af116fd6ee307b8ca6a"
        self.signature = 'q?\xd8p\r\x98w}3A\xc9/\x05\xd5\xf7\x19\xe1B-4\xcec\xe1\xa7\xb8\xb2&\xdbG\xd9\xc9\x81T\xfa\xdeX\xda\xd3\x11s\\H\xce\x82\xab\xcc\nP+*\x1eO\x02Q\x8f\xb0L\x11\xcd\x8fm\x10\xe7\xcc\xce\xc6\xd4hU\xf0d\x84\x8f\x82\xc3.G<\x1dt\x11\xd51\xb4\x13L t\x92\x02FO\x9e\x8e\xd7\x07#\xb9\xcd\x03)b\xffM\x13\x05v\xa43\xe9t\xbf\xfda\xa9\xb2K\x7f\x8b\xee\xd2\xa3\xb27\xb4\xa6\xc4\x88\xf5\xf9~}0\xdc\x8e\xfa\xf8q\xe6\x90\xf73w=\xd0\xff\xcbX\xdd\x10\x18e\x15\xec\xbb\xbf\xd6\x03\xc4\xf7\x86\x1fr\x11+\xbffmY\xff\x84q\xa1*{ \xca\xf2\nN\x8b\xfc\x85x3`\xab\xd5&\xb3\xb1\x03\xc6G5\xee\xebh\x91\xe3\xd5Y2\x96\x87\xe7\xd2\xaeu\xf7\xea\x80\x9c\xfc\x05HjZ\x81\xb7Qz\xd2p\x8e\xf6\xad\x0c\x9a\xf2\xdd"\x02\xf3\xab\xff2E\x03\xea\xd7W\x9b\xe2^`\xdc\xec5\x92z\xed\x02\xe7\xaa&'


    def test_load(self):
        cert_file = open(self.certificate_file)
        cert = cvmfs.Certificate(cert_file)
        cert_file.close()


    def test_get_openssl_x509(self):
        cert_file = open(self.certificate_file)
        cert = cvmfs.Certificate(cert_file)
        cert_file.close()
        x509 = cert.get_openssl_certificate()
        self.assertTrue(isinstance(x509, X509))


    def test_verify_message(self):
        cert_file = open(self.certificate_file)
        cert = cvmfs.Certificate(cert_file)
        cert_file.close()
        self.assertTrue(cert.verify(self.signature, self.message_digest))


    def test_verify_tampered_message(self):
        cert_file = open(self.certificate_file)
        cert = cvmfs.Certificate(cert_file)
        cert_file.close()
        self.assertFalse(cert.verify(self.signature, "I am Malory!"))


    def test_certificate_fingerprint(self):
        cert_file = open(self.certificate_file)
        cert = cvmfs.Certificate(cert_file)
        cert_file.close()
        self.assertEqual(self.fingerprint, cert.get_fingerprint())
