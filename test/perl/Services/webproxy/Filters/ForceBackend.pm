package Filters::ForceBackend;

######################################
# This filter is intended to be used for request
######################################

use strict;
use warnings;
use HTTP::Proxy::HeaderFilter::simple;

our $header = HTTP::Proxy::HeaderFilter::simple->new (
	sub {
		my $uri = $_[2]->uri;
		
		my $backend = $main::backend;
		my $host_port = (split /\//, $backend)[-1];
		
		$uri->host("$host_port") ;
		
		$_[2]->uri($uri);
	}
);

1;
