from __future__ import print_function

import math
import os
import re
import bisect
import socket
import cvmfs_api
import time
import threading

# Open the geodb.  Only import maxminddb here (and only once) because it
#  is not available in the unit test.
maxminddb = None
def open_geodb(dbname):
    global maxminddb
    if maxminddb is None:
        import maxminddb
    return maxminddb.open_database(dbname)

gidbpath1="/var/lib/cvmfs-server/geo/iplocation.mmdb"
gidbpath2="/var/lib/cvmfs-server/geo/GeoLite2-City.mmdb"
oldgidb=""
gidb=""
gireader=None
oldgireader=None
gichecktime=0
gimodtime=0

geo_cache_secs = 5*60   # 5 minutes

geo_cache_max_entries = 100000  # a ridiculously large but manageable number
namelookups = 0

# geo_cache entries are indexed by name and contain a tuple of
# (update time, geo record).  Caching DNS lookups is more important
# than caching geo information but it's simpler and slightly more
# efficient to cache the geo information.
geo_cache = {}

gilock = threading.Lock()
namelock = threading.Lock()

# look up geo info for an address
# Also periodically check for an update to the database and
#   reopen it if it changed
def lookup_geoinfo(now, addr):
    global gireader, oldgireader
    global gichecktime
    global gimodtime

    if gireader is None or now > gichecktime + geo_cache_secs:
        gilock.acquire()
        try:
            # gichecktime might have changed before acquiring the lock, look again
            if gireader is None or now > gichecktime + geo_cache_secs:
                if oldgireader is not None:
                    # By now we're sure nobody is still using the previous
                    #  gireader, so close it.  This delay avoids having to
                    #  acquire the lock for every lookup.
                    oldgireader.close()
                    oldgireader = None
                    print('cvmfs_geo: closed old ' + oldgidb)
                gichecktime = now
                try:
                    gidb = gidbpath1
                    statb = os.stat(gidb)
                except FileNotFoundError:
                    try:
                        gidb = gidbpath2
                        statb = os.stat(gidb)
                    except FileNotFoundError as e:
                        e.filename = gidbpath1 + ' or ' + gidbpath2
                        raise e
                modtime = statb.st_mtime
                if modtime != gimodtime:
                    # database was modified, reopen it
                    oldgireader = gireader
                    oldgidb = gidb
                    gireader = open_geodb(gidb)
                    gimodtime = modtime
                    print('cvmfs_geo: opened ' + gidb)
        finally:
            gilock.release()

    return gireader.get(addr)

# function came from http://www.johndcook.com/python_longitude_latitude.html
def distance_on_unit_sphere(lat1, long1, lat2, long2):

    if (lat1 == lat2) and (long1 == long2):
        return 0.0

    # Convert latitude and longitude to
    # spherical coordinates in radians.
    degrees_to_radians = math.pi/180.0

    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians

    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians

    # Compute spherical distance from spherical coordinates.

    # For two locations in spherical coordinates
    # (1, theta, phi) and (1, theta, phi)
    # cosine( arc length ) =
    #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
    # distance = rho * arc length

    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) +
           math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )

    # Remember to multiply arc by the radius of the earth
    # in your favorite set of units to get length.
    return arc

# Pattern including all allowed characters in addresses.
# The geoip api functions will further validate, but for paranoia's sake
#   (because I'm not sure how good the functions' error checking is), make
#   sure the names are limited to valid hostname characters.
# Include ':' for IPv6 addresses.
addr_pattern = re.compile('^[0-9a-zA-Z.:-]*$')

# Look up geo info for IPv4 or IPv6 address.
# Will return None if the address does not exist in the DB.
def addr_geoinfo(now, addr):
    if (len(addr) > 256) or not addr_pattern.search(addr):
        return None

    response = lookup_geoinfo(now, addr)
    if response == None:
        return None

    return response['location']

# Look up geo info by name (dns or ip address).  Try IPv4 first since that DB is
# better and most servers today are dual stack if they have IPv6.
# Store results in a cache.  Wsgi is multithreaded so need to lock
# accesses to the shared cache.
# Return geo info record or None if none found.
def name_geoinfo(now, hostname):
    global geo_cache
    if (len(hostname) > 256) or not addr_pattern.search(hostname):
        return None

    # ignore short names and non-FQDNs in lookups, since they will fail anyway:
    #TODO(vavolkl): The client currently cannot use an IPv6 address for this lookup;
    # should be supported. Allow it here already in preparation for this
    if not '.' in hostname and not ':' in hostname:
        # not an fqdn or IPv4 or IPv6 address
        return None
        
    global namelookups
    namelock.acquire()
    if hostname in geo_cache:
        (stamp, gir) = geo_cache[hostname]
        if now <= stamp + geo_cache_secs:
            # still good, use it
            namelock.release()
            return gir
        # update the timestamp so only one thread needs to wait
        #  when a lookup is slow
        geo_cache[hostname] = (now, gir)
    elif len(geo_cache) >= geo_cache_max_entries:
        # avoid denial of service by removing one arbitrary entry
        #   before we add one
        geo_cache.popitem()
    namelookups += 1
    namelock.release()

    ai = ()
    try:
        ai = socket.getaddrinfo(hostname,80,0,0,socket.IPPROTO_TCP)
    except:
        pass
    gir = None
    for info in ai:
        # look for IPv4 address first
        if info[0] == socket.AF_INET:
            gir = lookup_geoinfo(now, info[4][0])
            break
    if gir == None:
        # look for an IPv6 address if no IPv4 record found
        for info in ai:
            if info[0] == socket.AF_INET6:
                gir = lookup_geoinfo(now, info[4][0])
                break
    if gir != None:
        if 'location' in gir:
            gir = gir['location']
        else:
            gir = None

    namelock.acquire()
    if gir == None and hostname in geo_cache:
        # reuse expired entry
        gir = geo_cache[hostname][1]

    geo_cache[hostname] = (now, gir)
    namelock.release()

    return gir

# geo-sort list of servers relative to gir_rem
#   If trycdn is True, first try prepending "ip." to the name to get the
#      real IP address instead of a Content Delivery Network front end.
# return list of [onegood, indexes] where
#   onegood - a boolean saying whether or not there was at least
#      one valid looked up geolocation from the servers
#   indexes - list of numbers specifying the order of the N given servers
#    servers numbered 0 to N-1 from geographically closest to furthest
#    away compared to gir_rem
def geosort_servers(now, gir_rem, servers, trycdn=False):
    idx = 0
    arcs = []
    indexes = []

    onegood = False
    for server in servers:
        gir_server = None
        if trycdn:
            gir_server = name_geoinfo(now, "ip." + server)
        if gir_server is None:
            gir_server = name_geoinfo(now, server)

        if gir_server is None:
            # put it on the end of the list
            arc = float("inf")
        else:
            onegood = True
            arc = distance_on_unit_sphere(gir_rem['latitude'],
                                          gir_rem['longitude'],
                                          gir_server['latitude'],
                                          gir_server['longitude'])
            #print("distance between " + \
            #    str(gir_rem['latitude']) + ',' + str(gir_rem['longitude']) \
            #    + " and " + \
            #    server + ' (' + str(gir_server['latitude']) + ',' + str(gir_server['longitude']) + ')' + \
            #    " is " + str(arc))

        i = bisect.bisect(arcs, arc)
        arcs[i:i] = [arc]
        indexes[i:i] = [idx]
        idx += 1

    return [onegood, indexes]

# expected geo api URL:  /cvmfs/<repo_name>/api/v<version>/geo/<path_info>
#   <repo_name> is repository name
#   <version> is the api version number, typically "1.0"
#   <path_info> is <caching_string>/<serverlist>
#     <caching_string> can be anything to assist in ensuring that those
#       clients wanting the same answer get responses cached together;
#       typically the name of their shared proxy.  If this resolves to
#       a valid IP address, attempt to use that address as the source
#       IP rather than the address seen by the web server. The reason for
#       that is so it isn't possible for someone to poison a cache by
#       using a name for someone else's proxy.
#     <serverlist> is a comma-separated list of N server names
# response: a comma-separated list of numbers specifying the order of the N
#    given servers numbered 1 to N from geographically closest to furthest
#    away from the requester that initiated the connection (the requester
#    is typically the proxy)

def api(path_info, repo_name, version, start_response, environ):

    slash = path_info.find('/')
    if (slash == -1):
        return cvmfs_api.bad_request(start_response, 'no slash in geo path')

    caching_string = path_info[0:slash]
    servers = path_info[slash+1:].split(',')

    if caching_string == "_namelookups_":
        # this is a special debugging URL
        return cvmfs_api.good_request(start_response, str(namelookups) + '\n')

    # TODO(jblomer): Can this be switched to monotonic time?
    now = int(time.time())

    trycdn = False
    if 'HTTP_CF_CONNECTING_IP' in environ:
        # Request is coming from Cloudflare Content Delivery Network;
        #  servers are probably using Cloudflare too.
        trycdn = True

    gir_rem = None
    if caching_string.find('.'):
        # might be a FQDN, use it if it resolves to a geo record
        gir_rem = name_geoinfo(now, caching_string)

    if gir_rem is None:
        if 'HTTP_CF_CONNECTING_IP' in environ:
            # IP address of client connecting to Cloudflare
            gir_rem = addr_geoinfo(now, environ['HTTP_CF_CONNECTING_IP'])
        if gir_rem is None and 'HTTP_X_FORWARDED_FOR' in environ:
            # List of IP addresses forwarded through squid
            # Try the last IP, in case there's a reverse proxy squid
            #  in front of the web server.
            forwarded_for = environ['HTTP_X_FORWARDED_FOR']
            start = forwarded_for.rfind(' ') + 1
            if (start == 0):
                start = forwarded_for.rfind(',') + 1
            gir_rem = addr_geoinfo(now, forwarded_for[start:])
        if gir_rem is None and 'REMOTE_ADDR' in environ:
            # IP address connecting to web server
            gir_rem = addr_geoinfo(now, environ['REMOTE_ADDR'])

    if gir_rem is None:
        return cvmfs_api.bad_request(start_response, 'remote addr not found in database')

    if '+PXYSEP+' in servers:
        # first geosort the proxies after the separator and if at least one
        # is good, sort the hosts before the separator relative to that
        # proxy rather than the client
        pxysep = servers.index('+PXYSEP+')
        # assume backup proxies will not be behind a CDN
        onegood, pxyindexes = \
            geosort_servers(now, gir_rem, servers[pxysep+1:], False)
        if onegood:
            gir_pxy = name_geoinfo(now, servers[pxysep+1+pxyindexes[0]])
            if not gir_pxy is None:
                gir_rem = gir_pxy
        onegood, hostindexes = \
            geosort_servers(now, gir_rem, servers[0:pxysep], trycdn)
        indexes = hostindexes + list(pxysep+1+i for i in pxyindexes)
        # Append the index of the separator for backward compatibility,
        # so the client can always expect the same number of indexes as
        # the number of elements in the request.
        indexes.append(pxysep)
    else:
        onegood, indexes = geosort_servers(now, gir_rem, servers, trycdn)

    if not onegood:
        # return a bad request only if all the server names were bad
        return cvmfs_api.bad_request(start_response, 'no server addr found in database')

    response_body = ','.join(str(i+1) for i in indexes) + '\n'

    return cvmfs_api.good_request(start_response, response_body)

