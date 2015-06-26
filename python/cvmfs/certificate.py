#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import M2Crypto
from M2Crypto import EVP, X509, m2, util
from distutils.version import LooseVersion, StrictVersion

from _common import CompressedObject

class Certificate(CompressedObject):
    """ Wraps an X.509 certificate object as stored in CVMFS repositories """

    def __init__(self, certificate_file):
        CompressedObject.__init__(self, certificate_file)
        cert = X509.load_cert_string(self.get_uncompressed_file().read())
        self.openssl_certificate = cert

    def __str__(self):
        return "<Certificate " + self.get_fingerprint() + ">"

    def __repr__(self):
        return self.__str__()

    def get_openssl_certificate(self):
        """ return the certificate as M2Crypto.X509 object """
        return self.openssl_certificate


    def _get_fingerprint(self, algorithm='sha1'):
        """ Workaround for RHEL5 with ancient version of M2Crypto """
        if LooseVersion(M2Crypto.version) < StrictVersion("0.17"):
            der = self.openssl_certificate.as_der()
            md = EVP.MessageDigest(algorithm)
            md.update(der)
            digest = md.final()
            return hex(util.octx_to_num(digest))[2:-1].upper()
        else:
            return self.openssl_certificate.get_fingerprint()

    def _check_signature(self, pubkey, signature):
        """ Workaround for RHEL5 with ancient version of M2Crypto """
        if LooseVersion(M2Crypto.version) < StrictVersion("0.18"):
            return m2.verify_final(pubkey.ctx, signature, pubkey.pkey)
        else:
            return pubkey.verify_final(signature)

    def get_fingerprint(self, algorithm='sha1'):
        """ returns the fingerprint of the X509 certificate """
        fp = self._get_fingerprint()
        return ':'.join([ x + y for x, y in zip(fp[0::2], fp[1::2]) ])


    def verify(self, signature, message):
        """ verify a given signature to an expected 'message' string """
        pubkey = self.openssl_certificate.get_pubkey()
        pubkey.reset_context(md='sha1')
        pubkey.verify_init()
        pubkey.verify_update(message)
        return self._check_signature(pubkey, signature)
