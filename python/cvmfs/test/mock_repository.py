#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created by René Meusel
This file is part of the CernVM File System auxiliary tools.
"""

import base64
import datetime
import hashlib
import os
import StringIO
import tarfile
import threading

from M2Crypto import RSA

import SimpleHTTPServer
import SocketServer

from file_sandbox import FileSandbox

class CvmfsTestServer(SocketServer.TCPServer):
    allow_reuse_address = True
    def __init__(self, document_root, bind_address, handler):
        self.document_root = document_root
        SocketServer.TCPServer.__init__(self, bind_address, handler)

class CvmfsRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def translate_path(self, path):
        return os.path.normpath(self.server.document_root + os.sep + path)

    def log_message(self, msg_format, *args):
        pass


class MockRepository:
    """ Generates a mock CVMFS repository for unit testing purposes """
    repo_extract_dir = "repo"

    def __init__(self):
        self.running = False
        self.sandbox = FileSandbox("py_cvmfs_mock_repo_")
        self.repo_name = MockRepository.repo_name
        self._extract_dir = os.path.join(self.sandbox.temporary_dir,
                                         MockRepository.repo_extract_dir)
        self.dir = os.path.join(self._extract_dir, "cvmfs", self.repo_name)
        self._setup_repository()

    def __del__(self):
        if self.running:
            self._shut_down_http_server()


    def serve_via_http(self, port = 8000):
        self._spawn_http_server(self._extract_dir, port)
        self.url = "http://localhost:" + str(port) + "/cvmfs/" + self.repo_name


    def make_valid_whitelist(self):
        tomorrow = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        self._resign_whitelist(tomorrow)


    def make_expired_whitelist(self):
        yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=2)
        self._resign_whitelist(yesterday)


    def _resign_whitelist(self, expiry_date):
        old_whitelist = os.path.join(self.dir, ".cvmfswhitelist")
        new_whitelist = os.path.join(self.dir, ".cvmfswhitelist.new")
        wl_hash = hashlib.sha1()
        with open(new_whitelist, 'w+') as new_wl: # TODO: more elegant is Py 2.7
            with open(old_whitelist) as old_wl:
                pos = old_wl.tell()
                while True:
                    line = old_wl.readline()
                    if len(line) >= 3 and line[0:3] == 'E20': #fails in 85 years
                        line = 'E' + expiry_date.strftime("%Y%m%d%H%M%S") + '\n'
                    if line[0:2] == "--":
                        break
                    if pos == old_wl.tell():
                        raise Exception("Signature not found in whitelist")
                    wl_hash.update(line)
                    new_wl.write(line)
                    pos = old_wl.tell()
            new_wl.write("--\n")
            new_wl.write(wl_hash.hexdigest())
            new_wl.write("\n")
            key = RSA.load_key(self.master_key)
            sig = key.private_encrypt(wl_hash.hexdigest(), RSA.pkcs1_padding)
            new_wl.write(sig)
        os.rename(new_whitelist, old_whitelist)


    def _setup_repository(self):
        self.sandbox.create_directory(self._extract_dir)
        repo = StringIO.StringIO(base64.b64decode(MockRepository.repo_data))
        repo_tar = tarfile.open(None, "r:gz", repo)
        repo_tar.extractall(self._extract_dir)
        pubkey = self.sandbox.write_to_temporary(MockRepository.repo_pubkey)
        self.public_key = pubkey
        privkey = self.sandbox.write_to_temporary(MockRepository.repo_privkey)
        self.private_key = privkey
        mkey = self.sandbox.write_to_temporary(MockRepository.repo_masterkey)
        self.master_key = mkey


    def _spawn_http_server(self, document_root, port):
        handler = CvmfsRequestHandler
        address = ("localhost", port)
        self.httpd = CvmfsTestServer(document_root, address, handler)
        self.httpd_thread = threading.Thread(target=self.httpd.serve_forever)
        self.httpd_thread.setDaemon(True)
        self.httpd_thread.start()
        self.running = True

    def _shut_down_http_server(self):
        self.httpd.shutdown()
        self.url = None


################################################################################


    repo_name = "test.cern.ch"

    repo_pubkey = '\n'.join([
'-----BEGIN PUBLIC KEY-----',
'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAueS/yBwR4UlRsgkv7hcM',
'ljvt/KyrhkI5y7n7ksLBFumjPhieaWz3L44s4Y1dUJ2H8krRqLXVjQ0X5x/F/nCH',
'erxxjuei4Vu9yG6BFqow0ZdmjqJzU4swRylBkSjf4QVOVSxUcbd7sL2QVbRH9g+C',
'hQ42pB+PD0CcEZp3VEsFV4wI9IY7EMRUC6dwM+LG0jubvhGbvlaqtufGXDESRJEu',
'RM76cnfxh/0qui8Vbs93St2VhsahJWcNGdeIaXlVsMyI77u1QhztPvF39+oDWEsW',
'2xAkfJ5d6NZJdrmRBD3agh5Zj8DuK0VttsvXwsSMw5gwBSRGLJIB8qXf254uouuq',
'lQIDAQAB',
'-----END PUBLIC KEY-----',
''
        ])

    repo_privkey = '\n'.join([
'-----BEGIN RSA PRIVATE KEY-----',
'MIIEowIBAAKCAQEA5TQNCMPftxI8Z27ReqQKgi1MNNmfW/l0Ns1Ax2KxspBYnbE3',
'FBhC1/B/uzIhW8mrYcXlFQ0400WfJHC+/pquDR/IXCrWB+1dyz9dp/7l4HcxcDNH',
'fc/g2hSvtdcw1ZCHNGONcCOxOYE3Rx10eZjviQZiGkzVkHwpnjgWNxbT7bkIV6iu',
'R8YWNUfGABwhTB5diq3UPtw5LEwYYj/CKFR9BkTK0VmZJbXV5bAvGQyINfT5296r',
'XJWD4WItJJ95R/zkBQgBjvLTNpFPRiOln8FTFpEDVWdteezDDXGWNWma3jYwvJg8',
'PBZEEcUmZBDQT9k0nfRizhsTyO7xzDmcAPfVHQIDAQABAoIBAB1JV1kFXjKQO/Oj',
'b1TSXR1hGFmwbPJdn4HZHCvd6oK8evY7TKRerTvWWRvcPfLyg9mMZccY12f3f2wy',
'k9UIgrDenMVaG9sLc26i/B6ZLVpPIJwLkVj8FOkIt6LuiijfvMbu6YWoqd6FKkEF',
'/HoFFqZVkHd31doOY2r6E6yaWB4JxqXcNJLAQRnm0mCytZUkzHodr+Tsy1GkSJC2',
'I3saPSIG5u4TL3XVsIBs5mE47FLK/YKRHJyp3tTLgTkhDvpLPEJC8ij1oWJ4PSzo',
'jeaK8xpv+LAjqu8brL9WZQI812Fmx+nPncSmKZrmjAYr4HaZ4aAgsWKOnqnyzrZv',
'QSao9EECgYEA8p1g/4iZVXkUjNVL6a+Cmj+g6+ji9wPXDOXFiwlOgXCDRFxunaMM',
'ZbZtxe26XHQdVm4e2hC4Zv2nn7Sn9gGg+7NCso5Y5qDH2HMJ/w+ya6XUIcE/YDMb',
'PqK6llLqsyLg+nrtV0ZkA5UN8Rlp116fWE0ensszH9qF43pB+7m8ANkCgYEA8dlA',
'sv9ZJW4/MtK8dbxzmrpyCRbLFsuYYN7k1ybrUVvK6WpGljqKbduTJ3/XC6qhJuWR',
'YAH9aTwwvRvEJor0EnYc+QpEnv5KHfNkKFU2x3tnnYZT24t2PLpY9AECjgeSUDk+',
'kHQzUGmig5eSSYMpGdLvRtD0BYXQLoD8eAy3y+UCgYEAtQ0jFK7Qlotr/YkzRGm4',
'kfmH0mUR8vqHolVZ7N7+GfRn0T0VQ0go+UKBeuJkX5g7SIOXPG6b3ifOzozXhutC',
'QnNNA8jcqQc0+98lh5UkNdcjjikTbWvWGhEAIywvf404zVOtCKM8AbxbEiA/7vvq',
'989dWW0UcuH1ZoOW+A5sMUkCgYB/E+zPISUyac+DYP/tzWvhLX6mD/f+rlQO8o/E',
'DYswYM8p/tHANlpuhyW3Z5ETbEDpM09D50fEeAAUHfbfWbwNx0pKAX81G+DOBAno',
't33lK46yUtbVUV57Yl9DNxSklI3o4Wtic+xSoG7oPkh7oBOEojVgPIM8M6fEB7qh',
'Se15kQKBgBQ8/Z4A95yoGlv1RYOBuZpOpEtbgi/NiJdRXnzrmQ1m31heRbkfu3w0',
'5WzlYjaQQ3g3rsh+0Ond9bLFcZor6azcPSsu+cjC3Gsxm/0KZKPAroi73Gd4O0QH',
'ih/vJDlTHRS2ArfdYc9cUYTFvs8YuLy7y9Uho35ey6PLX6CEsJel',
'-----END RSA PRIVATE KEY-----',
''
])

    repo_masterkey = '\n'.join([
'-----BEGIN RSA PRIVATE KEY-----',
'MIIEowIBAAKCAQEAueS/yBwR4UlRsgkv7hcMljvt/KyrhkI5y7n7ksLBFumjPhie',
'aWz3L44s4Y1dUJ2H8krRqLXVjQ0X5x/F/nCHerxxjuei4Vu9yG6BFqow0ZdmjqJz',
'U4swRylBkSjf4QVOVSxUcbd7sL2QVbRH9g+ChQ42pB+PD0CcEZp3VEsFV4wI9IY7',
'EMRUC6dwM+LG0jubvhGbvlaqtufGXDESRJEuRM76cnfxh/0qui8Vbs93St2Vhsah',
'JWcNGdeIaXlVsMyI77u1QhztPvF39+oDWEsW2xAkfJ5d6NZJdrmRBD3agh5Zj8Du',
'K0VttsvXwsSMw5gwBSRGLJIB8qXf254uouuqlQIDAQABAoIBAASWKUk1sBc/6N0c',
'rusP9IaMaf3PANhqL+Tf7N4dIgh/sUBp+Rae0qaAuojCJShFCsKmp++itOcrCIjy',
'Vr9FZYJYvfCJtJIc4lzcpSC7CENTmfsw9Ol9yK4ozW5YdNWnfNxLILZBkbK1qqcC',
'sLfYgB7qT9zSzoPQ00j357PTugkD56eiJcNZu80nRy0Ud3D/3dDFJADF1hQkebwu',
'82NLqNQnTO2/KF1fJLgsIU3ymMdOV68k9rjtGfLRoK4qfX0lb8BNrAY2urPzU0yV',
'Y2unrWWbmWT2lDOIqRCfLbGSQuVfLbY7JOq+PwA+H7C2Py6GQLuFi8t5DTVuGke/',
'NtZpHkECgYEA9u+OPbZLISvmGNFZg4hn6k8PfH0GLrEcx0tPv4ReONqwXVobeYKX',
'/x0b6o2BC0bmICMjnGsDrZSZj2wiXAYknSQCxMAGAj0PoBI9KQNU1Simwb/IA0EE',
'd+c6BdR0YdVIQ7esSNaCaAb0zX1/y98U7HOQ2/ornhAM4wKKRtwykMUCgYEAwLeV',
'IvRHnwXls8kux+KOEAHGoWf4KqaOYUbqktSdVB5DziqN2Ktj46/wQxcmaS5gbMNR',
'B+lvveP7t3qKzMMMeBtKEKou1lGC+K7yWo//v1st25p8j9Ue0xlaw5ZiVRyYzZYV',
'uwnaBNFiNk8YH+to8UdwYGDPuNNZjE7JuFcdr5ECgYEAtsTKWBzj8LJoRXg2M+ez',
'WjaYNMDo4YhPz6aLaSpU/unGXeICseYaEEYAUpPXrnwUejbn9a8zcrepDQGxUMFv',
'OivcLLof+GovdX/qar+/e2HyQzdqmBX4c7LePFBqr7rIGO8KgoLa1JpJeQrpmwEL',
'oJNM5bR9sikZELDhnd7/Qi0CgYAV8VEzx6iX/K3oyJFhBPSz8d/R5OqmwIwZm19+',
'FGNNfpytzr6T2v/mntO2b95Zv4QPHjYNtpCYiGrSu0sugU7cJg9K0nW+xU0qT5Ec',
'qqSt/w27oV1paxS1aH+jIW5Uzoq/bcVPpJGEVurd0CepCr7KKh4rexprqvTZOudQ',
'6+pfYQKBgBmC5quiKh2ILLh5IJ1g5UXQPzFgE6J9XcVY3BumXBr41g2CYPC5oklM',
'v5PZ3wY76x8O+2S+sYBKzDHOv3Q8AJPC2PEIJORzTK6XfIetpnN3TR0LZvHiUpES',
'hmCojC2QE3Y7i+XTL2d9rbXLSIbMEWDHdBHKzTWczDIDo+tFPEFo',
'-----END RSA PRIVATE KEY-----',
''
])

    repo_data = '\n'.join([
'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',
''
        ])
