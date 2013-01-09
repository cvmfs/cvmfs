package Filters::RecordTransfer;

######################################
# This filter is intended to be used for response
######################################

use strict;
use warnings;
use HTTP::Proxy::BodyFilter::simple;

my $size = 0;
   
our $body = HTTP::Proxy::BodyFilter::simple->new (
	sub {
		my $record_file = '/tmp/transferred_data';
		
		if (-e $record_file) {
			my $fh;
			open($fh, '<', $record_file);
			my $actual_size = $fh->getline;
			close($fh);
			$actual_size += length ${ $_[1] };
			open($fh, '>', $record_file);
			print $fh $actual_size;
			close($fh);
		}
		else {
			open (my $fh, '>', $record_file);
			print $fh length $ { $_[1] };
			close($fh);
		}
	}
);

1;
