#!/usr/bin/python                                                               

import sys
import re
from math import floor
from datetime import datetime

ac=21   # Number of accounts
bb=100 # Number of buckets
bucket_body_name="lhcb"
servername="olhw-s3.cern.ch"

# Gets the account name based bucket number
def get_bucket_name(bucket_number):
    account=(bucket_number%ac)+1;
    bucket=floor(bucket_number/ac)+1;
    return bucket_body_name+"-%d-%d" % (account, bucket)

# Select correct bucket algorithm
# e.g. 43123 filename
# mod(0x431+0x23,99*18)
def select_bucket(rem_filename,f):
    use_bucket = 0;
    number_of_buckets = bb*ac; # Number of available buckets             
    cutlength= 3;              # Analyse filename in parts of this length
    hex_filename = "";         # Filename with only valid hex-symbols     

    # Accept only hex chars                                                    
    for i in range(0, len(rem_filename)): 
      w = ord(rem_filename[i]);
      if (w >= 48 and w <= 57) or (w >=65  and w <= 70) or (w >=97  and w <= 102):
        hex_filename += chr(w);
      else:
        hex_filename += '6';
    #print "Hex:    ", hex_filename
    f.write("Hex:    ")
    f.write(hex_filename)
    f.write("\n");
    f.flush()

    # Calculate number based on the filename                                   
    xt = 0; x = 0;
    while True:
      if len(hex_filename) < cutlength:
        break;
      xt = int(hex_filename[:cutlength], 16)
      x += xt;
      hex_filename = hex_filename[cutlength:];
    
    if len(hex_filename) > 0:
      xt = int(hex_filename, 16)
      x += xt;
    
    # Choose the bucket wih modulo                                             
    use_bucket = x%number_of_buckets;

    return use_bucket;

# Select correct bucket algorithm
# e.g. 43123 filename
# mod(0x431+0x23,99*18)
def select_bucket_hex(rem_filename):
    use_bucket = 0;
    number_of_buckets = bb*ac; # Number of available buckets             
    cutlength= 3;              # Analyse filename in parts of this length
    hex_filename = "";         # Filename with only valid hex-symbols     

    # Accept only hex chars                                                    
    for i in range(0, len(rem_filename)): 
      w = ord(rem_filename[i]);
      if (w >= 48 and w <= 57) or (w >=65  and w <= 70) or (w >=97  and w <= 102):
        hex_filename += chr(w);
    print "Hex:    ", hex_filename

    # Calculate number based on the filename                                   
    xt = 0; x = 0;
    while True:
      if len(hex_filename) < cutlength:
        break;
      xt = int(hex_filename[:cutlength], 16)
      x += xt;
      hex_filename = hex_filename[cutlength:];
    
    if len(hex_filename) > 0:
      xt = int(hex_filename, 16)
      x += xt;
    
    # Choose the bucket wih modulo                                             
    use_bucket = x%number_of_buckets;

    return use_bucket;

# Select correct bucket algorithm
def select_bucket_kiss(rem_filename):
  number_of_buckets = bb*ac; # Number of available buckets             
  return int(rem_filename)%number_of_buckets;

# URL mangling function
def mangle_url(url_prefix, riak_bucket, url_suffix):
  return "http://%s.%s/%s" % (riak_bucket, url_prefix, url_suffix)

def mangle_url_nosubdomain(url_prefix, riak_bucket, url_suffix):
  return "http://%s/%s/%s" % (url_prefix, riak_bucket, url_suffix)

#regexp = re.compile('^http://(.*?).(olhw-.*?)/(.*?)$')
regexp = re.compile('^http://(.*?)/(.*?)/(.*?)$')
#regexpnob = re.compile('^(http://[^/]*?)/([^/]*+)$')
regexpnob_dirs = re.compile('^(http://[^/]+)/([^/]+)$')
regexpnob = re.compile('^(http://[^/]+)/(.*)$')

#url='http://ssheikki-cvmfs10-11.olhw-s3.cern.ch/koe-filu.txt'
#url='http://ssheikki-cvmfs10-11.olhw-osc7.cern.ch/koe-filu.txt'
#url='http://ssheikki-cvmfs3.olhw-s3.cern.ch/koe-filu.txt'
#url='http://ssheikki-cvmfs3.olhw-s3.cern.ch/43123'
#url='http://ssheikki-cvmfs3.olhw-s3.cern.ch/7424'

f = open('/tmp/url_rewrite.log', 'a')
f.write("url_rewrite started 9\n")
f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
f.write("\n")
f.flush()

while True:
  # read line from squid
  line = sys.stdin.readline()
  if not line:
    break

  # log the request
  f.write(line)
  f.flush()

  # analyze the squid request
  parts = line.split(" ")
  url2  = parts[0]
  url3  = url2.replace("//", "/")
  url   = 'http://' + url3[6:]
  f.write(url);
  f.write("\n");
  f.flush()

  match_result = regexpnob.match(url)

  # Normal url with bucket name and all (could be removed!)
  if (match_result):
    url_prefix  = match_result.group(1)
    #riak_bucket = match_result.group(2)
    url_suffix  = match_result.group(2)

    #pnum = select_bucket_kiss(url_suffix)
    pnum = select_bucket(url_suffix,f)
    bname = get_bucket_name(pnum)

    #new_url = mangle_url_nosubdomain(url_prefix, bname, url_suffix)
    new_url = mangle_url_nosubdomain(servername, bname, url_suffix)
    f.write("bname=");
    f.write(bname);
    f.write("\n");
    f.write("new_url='302:");
    f.write(new_url);
    f.write("'\n");
    f.flush()
    sys.stdout.write("302:"+new_url)
  else:
    old_url=url
    #    sys.stdout.write(url)
    #sys.stdout.write("http://olhw-s3.cern.ch/ 127.0.0.1/localhost - GET myip=127.0.0.1 myport=80")
    #old_url="http://olhw-s3.cern.ch/"
    sys.stdout.write(old_url);
    f.write("old_url='")
    f.write(old_url)
    f.write("'\n")
    f.flush()

  f.write("end\n\n")
  f.flush()

  # send the result back to squid
  sys.stdout.write("\n")
  sys.stdout.flush()

f.close()

