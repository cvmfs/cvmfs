# Based on https://github.com/cvmfs-contrib/varnish-cvmfs/blob/main/cvmfs.vcl
# for CI-local forward-proxy use inside GitHub Actions.
# Keep local adaptations in start-varnish.sh rather than rewriting the VCL.

#
# CVMFS VCL (v1.1)
#
# Tested with CVMFS 2.11 and Varnish Cache 6/7
#

vcl 4.1;

import dynamic;

backend default none;

sub vcl_init
{
	new proxy = dynamic.director();
}



sub vcl_recv
{
	set req.http.X-Proxy-Host = regsub(req.http.Host, ":.*$", "");

	# Parse the port out if it exists
	if (req.http.Host ~ ":") {
		set req.http.X-Proxy-Port = regsub(req.http.Host, "^.*:", "");
	} else {
		set req.http.X-Proxy-Port = "80";
	}

	set req.backend_hint = proxy.backend(req.http.X-Proxy-Host, req.http.X-Proxy-Port);


  unset req.http.CVMFS-Mutable;
  unset req.http.CVMFS-Immutable;
  unset req.http.CVMFS-Metadata;
  unset req.http.CVMFS-Published;
  unset req.http.CVMFS-Whitelist;
  unset req.http.CVMFS-Dirtab;
  unset req.http.CVMFS-Data;
  unset req.http.CVMFS-Catalog;
  unset req.http.CVMFS-History;
  unset req.http.CVMFS-Partial;
  unset req.http.CVMFS-Temporary;
  unset req.http.CVMFS-Certificate;
  unset req.http.CVMFS-Metainfo;
  unset req.http.CVMFS-External;

  set req.http.CVMFS-Status = "NONE";

  # CVMFS objects
  if (req.url ~ "\/\.cvmfs[0-9a-z]*$") {
    set req.http.CVMFS-Mutable = "true";
    set req.http.CVMFS-Metadata = "true";
    if (req.url ~ "\/\.cvmfspublished$") {
      set req.http.CVMFS-Published = "true";
    } else if (req.url ~ "\/\.cvmfswhitelist$") {
      set req.http.CVMFS-Whitelist = "true";
    } else if (req.url ~ "\/\.cvmfsdirtab$") {
      set req.http.CVMFS-Dirtab = "true";
    }
  } else if (req.url ~ "\/data\/[0-9a-z]{2}\/[a-z0-9]{38}[A-Z]?$") {
    set req.http.CVMFS-Immutable = "true";
    set req.http.CVMFS-Data = "true";
    if (req.url ~ "[A-Z]$") {
      set req.http.CVMFS-Metadata = "true";
      if (req.url ~ "C$") {
        set req.http.CVMFS-Catalog = "true";
      } else if (req.url ~ "H$") {
        set req.http.CVMFS-History = "true";
      } else if (req.url ~ "P$") {
        set req.http.CVMFS-Partial = "true";
      } else if (req.url ~ "T$") {
        set req.http.CVMFS-Temporary = "true";
      } else if (req.url ~ "X$") {
        set req.http.CVMFS-Certificate = "true";
      } else if (req.url ~ "M$") {
        set req.http.CVMFS-Metainfo = "true";
      }
    }
  }
  if (!req.http.CVMFS-Mutable && !req.http.CVMFS-Immutable) {
    set req.http.CVMFS-External = "true";
  }

  if (req.method == "GET" || req.method == "HEAD") {
    return (hash);
  }

  return (pass);
}

sub vcl_hash {
  hash_data(req.url);
  return (lookup);
}

sub vcl_hit {
  set req.http.CVMFS-Status = "HIT";
}

sub vcl_miss {
  set req.http.CVMFS-Status = "MISS";
}

sub vcl_pass {
  set req.http.CVMFS-Status = "PASS";
}

sub vcl_backend_response {
  if (beresp.status == 200) {
    if (bereq.http.CVMFS-Published) {
      set beresp.ttl = 10s;
      set beresp.grace = 10s;
    } else if (bereq.http.CVMFS-Mutable) {
      set beresp.ttl = 5m;
      set beresp.grace = 5m;
    } else if (bereq.http.CVMFS-Immutable || bereq.http.CVMFS-External) {
      set beresp.ttl = 30d;
      set beresp.grace = 7d;
    }
    set beresp.keep = 1y;
  } else {
    set beresp.ttl = 1s;
    set beresp.grace = 0s;
    set beresp.keep = 0s;
  }
}

sub vcl_backend_error {
  if (beresp.status == 503) {
    return (retry);
  }
}

sub vcl_deliver {
  set resp.http.CVMFS-Varnish = "cvmfs.vcl";

  if (resp.http.CVMFS-Status) {
    set resp.http.CVMFS-Status = resp.http.CVMFS-Status + ", ";
  }
  if (req.http.CVMFS-Status ~ "HIT|MISS") {
    set resp.http.CVMFS-Status = resp.http.CVMFS-Status + server.identity + " " + req.http.CVMFS-Status +
      "(" + obj.hits + ")";
  } else {
    set resp.http.CVMFS-Status = resp.http.CVMFS-Status + server.identity + " " + req.http.CVMFS-Status;
  }

  if (obj.uncacheable) {
    set resp.http.CVMFS-Uncacheable = "true";
  } else {
    set resp.http.CVMFS-TTL = obj.ttl;
  }
}