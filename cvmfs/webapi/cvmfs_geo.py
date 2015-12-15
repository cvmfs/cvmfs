import math
import string
import re
import bisect
import cvmfs_api

import GeoIP

gi = GeoIP.open("/var/lib/cvmfs-server/geo/GeoLiteCity.dat", GeoIP.GEOIP_STANDARD)
gi6 = GeoIP.open("/var/lib/cvmfs-server/geo/GeoLiteCityv6.dat", GeoIP.GEOIP_STANDARD)

positive_expire_secs = 60*60  # 1 hour

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

# expected geo api URL:  /cvmfs/<repo_name>/api/v<version>/geo/<path_info>
#   <repo_name> is repository name
#   <version> is the api version number, typically "1.0"
#   <path_info> is <caching_string>/<serverlist>
#     <caching_string> can be anything to assist in ensuring that those
#       clients wanting the same answer get responses cached together;
#       typically the name of their shared proxy
#     <serverlist> is a comma-separated list of N server names
# response: a comma-separated list of numbers specifying the order of the N
#    given servers numbered 1 to N from geographically closest to furthest
#    away from the requester that initiated the connection (the requester
#    is typically the proxy)

def api(path_info, repo_name, version, start_response, environ):

    start = string.find(path_info, '/') + 1
    if (start == 0):
        return cvmfs_api.bad_request(start_response, 'no slash in geo path')

    servers = string.split(path_info[start:], ",")

    rem_addr = ''
    if 'HTTP_X_FORWARDED_FOR' in environ:
        forwarded_for = environ['HTTP_X_FORWARDED_FOR']
        start = string.rfind(forwarded_for, ' ') + 1
        if (start == 0):
            start = string.rfind(forwarded_for, ',') + 1
        rem_addr = forwarded_for[start:]
    else:
        if 'REMOTE_ADDR' in environ:
            rem_addr = environ['REMOTE_ADDR']

    if (len(rem_addr) < 256) and addr_pattern.search(rem_addr):
        if rem_addr.find(':') != -1:
            gir_rem = gi6.record_by_addr_v6(rem_addr)
        else:
            gir_rem = gi.record_by_addr(rem_addr)
    else:
        gir_rem = None

    if gir_rem is None:
        return cvmfs_api.bad_request(start_response, 'remote addr not found in database')

    idx = 1
    arcs = []
    indexes = []

    onegood = False
    for server in servers:
        if (len(server) < 256) and addr_pattern.search(server):
            # try IPv4 first since that DB is better and most servers
            #    today are dual stack if they have IPv6
            gir_server = gi.record_by_name(server)
            if gir_server is None:
                gir_server = gi6.record_by_name_v6(server)
        else:
            gir_server = None

        if gir_server is None:
            # put it on the end of the list
            arc = float("inf")
        else:
            onegood = True
            arc = distance_on_unit_sphere(gir_rem['latitude'],
                                          gir_rem['longitude'],
                                          gir_server['latitude'],
                                          gir_server['longitude'])

        i = bisect.bisect(arcs, arc)
        arcs[i:i] = [ arc ]
        indexes[i:i] = [ str(idx) ]
        idx += 1

    if not onegood:
        # return a bad request only if all the server names were bad
        return cvmfs_api.bad_request(start_response, 'no server addr found in database')

    response_body = string.join(indexes, ',') + '\n'

    status = '200 OK'
    response_headers = [('Content-Type', 'text/plain'),
                  ('Cache-Control', 'max-age=' + str(positive_expire_secs)),
                  ('Content-Length', str(len(response_body)))]
    start_response(status, response_headers)

    return [response_body]

