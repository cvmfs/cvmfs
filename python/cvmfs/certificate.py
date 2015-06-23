#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

from M2Crypto import X509

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


    def get_fingerprint(self, algorithm='sha1'):
        """ returns the fingerprint of the X509 certificate """
        fp = self.openssl_certificate.get_fingerprint(algorithm)
        return ':'.join([ x + y for x, y in zip(fp[0::2], fp[1::2]) ])


    def verify(self, signature, message):
        """ verify a given signature to an expected 'message' string """
        pubkey = self.openssl_certificate.get_pubkey()
        pubkey.reset_context(md='sha1')
        pubkey.verify_init()
        pubkey.verify_update(message)
        return pubkey.verify_final(signature)
