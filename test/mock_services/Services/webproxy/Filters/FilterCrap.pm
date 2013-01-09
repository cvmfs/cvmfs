package Filters::FilterCrap;

######################################
# This filter is intended to be used for response
######################################

use strict;
use warnings;
use HTTP::Proxy::HeaderFilter::simple;
use HTTP::Proxy::BodyFilter::simple;
use File::Slurp;
use File::MimeInfo;

our $header = HTTP::Proxy::HeaderFilter::simple->new (
	sub {
		my $mimetype = mimetype('/tmp/cvmfs.faulty');
		$_[2]->code( 700 );
		$_[2]->message ( 'Crap' );
		$_[1]->header ( Content_Type => "$mimetype; charset=iso-8859-1");
    }
);
    
our $body = HTTP::Proxy::BodyFilter::simple->new (
	sub {
		my ( $self, $dataref, $message, $protocol, $buffer ) = @_;
		unless (defined ($buffer)){
			$$dataref = read_file( '/tmp/cvmfs.faulty', { binmode => ':raw' } );
		}
	}
);

1;
