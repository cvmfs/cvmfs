"""
An extension of the Python stdlib SimpleHTTPServer module, to
support the "Range" header in HTTP requests, as needed by iOS Safari
to support (some) MP3s.

Some methods are modifications to the original SimpleHTTPServer that is
part of the Python stdlib.  This uses the versions that ship with Python
2.7 on Fedora 15.

Licensed under BSD 2-Clause License


Copyright (c) 2012, John Smith <code@john-smith.me>
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

    Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


"""
__version__ = "0.2"

__author__ = "John Smith <code@john-smith.me>"

import os
import BaseHTTPServer
import SimpleHTTPServer

# Additions for handling Range: header
import logging
import re

class InvalidRangeHeader(Exception):
    pass

def parse_range_header(range_header, total_length):
    """
    Return a 2-element tuple containing the requested range offsets
    in bytes.
    - range_header is the HTTP header sans the "Range:" prefix
    - total_length is the length in bytes of the requested resource
      (needed to calculate offsets for a 'n bytes from the end' request
    If no Range explicitly requested, returns (None, None)
    If Range header could not be parsed, raises InvalidRangeHeader
    (which could either be handled as a user
    request failure, or the same as if (None, None) was returned
    """
    # range_header = self.headers.getheader("Range")
    if range_header is None or range_header == "":
        return (None, None)
    if not range_header.startswith("bytes="):
        # logging.error("Don't know how to parse Range: %s [1]" %
        #                (range_header))
        raise InvalidRangeHeader("Don't know how to parse non-bytes Range: %s" %
                                 (range_header))
    regex = re.compile(r"^bytes=(\d*)\-(\d*)$")
    rangething = regex.search(range_header)
    if rangething:
        r1 = rangething.group(1)
        r2 = rangething.group(2)
        logging.debug("Requested range is [%s]-[%s]" % (r1, r2))

        if r1 == "" and r2 == "":
            # logging.warning("Requested range is meaningless")
            raise InvalidRangeHeader("Requested range is meaningless")

        if r1 == "":
            # x bytes from the end of the file
            try:
                final_bytes = int(r2)
            except ValueError:
                raise InvalidRangeHeader("Invalid trailing range")
            return (total_length-final_bytes, total_length - 1)

        try:
            from_val = int(r1)
        except ValueError:
            raise InvalidRangeHeader("Invalid starting range value")
        if r2 != "":
            try:
                end_val = int(r2)
            except ValueError:
                raise InvalidRangeHeader("Invalid ending range value")
            return (from_val, end_val)
        else:
            return (from_val, total_length - 1)
    else:
        raise InvalidRangeHeader("Don't know how to parse Range: %s" %
                                 (range_header))


class HTTPRangeRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    """
    Extension of SimpleHTTPServer.SimpleHTTPRequestHandler to support
    the Range header in HTTP requests.  (As needed for serving certain
    MP3 files to Mobile Safari.
    """

    server_version = "HTTPRangeServer/" + __version__

    def do_GET(self):
        """Serve a GET request."""
        f = self.send_head()
        if f:
            if self.range_from is not None and self.range_to is not None:
                self.copy_chunk(f, self.wfile)
            else:
                self.copyfile(f, self.wfile)
            f.close()

    def copy_chunk(self, in_file, out_file):
        """
        Copy a chunk of in_file as dictated by self.range_[from|to]
        to out_file.
        NB: range values are inclusive so 0-99 => 100 bytes
        Neither of the file objects are closed when the
        function returns.  Assumes that in_file is open
        for reading, out_file is open for writing.
        If range_tuple specifies something bigger/outside
        than the size of in_file, out_file will contain as
        much content as matches.  e.g. with a 1000 byte input,
        (500, 2000) will create a 500 byte long file
        (2000, 3000) will create a zero length output file
        """

        in_file.seek(self.range_from)
        # Add 1 because the range is inclusive
        left_to_copy = 1 + self.range_to - self.range_from

        bytes_copied = 0
        while bytes_copied < left_to_copy:
            read_buf = in_file.read(left_to_copy)
            if len(read_buf) == 0:
                break
            out_file.write(read_buf)
            bytes_copied += len(read_buf)
        return bytes_copied


    def send_head(self):
        """Common code for GET and HEAD commands.

        This sends the response code and MIME headers.

        Return value is either a file object (which has to be copied
        to the outputfile by the caller unless the command was HEAD,
        and must be closed by the caller under all circumstances), or
        None, in which case the caller has nothing further to do.

        """
        path = self.translate_path(self.path)
        f = None
        if os.path.isdir(path):
            if not self.path.endswith('/'):
                # redirect browser - doing basically what apache does
                self.send_response(301)
                self.send_header("Location", self.path + "/")
                self.end_headers()
                return None
            for index in "index.html", "index.htm":
                index = os.path.join(path, index)
                if os.path.exists(index):
                    path = index
                    break
            else:
                return self.list_directory(path)
        ctype = self.guess_type(path)
        try:
            # Always read in binary mode. Opening files in text mode may cause
            # newline translations, making the actual size of the content
            # transmitted *less* than the content-length!
            f = open(path, 'rb')
        except IOError:
            self.send_error(404, "File not found")
            return None

        fs = os.fstat(f.fileno())
        total_length = fs[6]
        try:
            self.range_from, self.range_to = parse_range_header(
                self.headers.getheader("Range"), total_length)
        except InvalidRangeHeader, e:
            # Just serve them the whole file, although it's possibly
            # more correct to return a 4xx error?
            logging.warning("Range header parsing failed, "
                            "serving complete file")
            self.range_from = self.range_to = None

        if self.range_from is not None or self.range_to is not None:
            self.send_response(206)
            self.send_header("Accept-Ranges", "bytes")
        else:
            self.send_response(200)
        self.send_header("Content-Type", ctype)
        if self.range_from is not None or self.range_to is not None:
            # TODO: Should also check that range is within the file size
            self.send_header("Content-Range",
                             "bytes %d-%d/%d" % (self.range_from,
                                                 self.range_to,
                                                 total_length))
            # Add 1 because ranges are inclusive
            self.send_header("Content-Length",
                             (1 + self.range_to - self.range_from))
        else:
            self.send_header("Content-Length", str(total_length))

        mtime = None
        try:
            mtime = self.date_time_string(fs.st_mtime)
        except TypeError:
            mtime = self.date_time_string()  # HOTFIX: for Python <= 2.4 without
                                             #         the timestamp parameter
        self.send_header("Last-Modified", mtime)
        self.end_headers()
        return f

def test(HandlerClass = HTTPRangeRequestHandler,
         ServerClass = BaseHTTPServer.HTTPServer):
    BaseHTTPServer.test(HandlerClass, ServerClass)

if __name__ == '__main__':
    test()
