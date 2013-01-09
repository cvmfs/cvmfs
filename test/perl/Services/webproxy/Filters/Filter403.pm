package Filters::Filter403;

use strict;
use warnings;
use HTTP::Proxy::HeaderFilter::simple;
use HTTP::Proxy::BodyFilter::simple;

our $header = HTTP::Proxy::HeaderFilter::simple->new (
	sub {
		$_[2]->code( 403 );
		$_[2]->message ( 'Forbidden' );
    }
);
    
our $body = HTTP::Proxy::BodyFilter::simple->new (
	sub {
		my ( $self, $dataref, $message, $protocol, $buffer ) = @_;
		unless (defined ($buffer)){
			my $html = 
					'<!DOCTYPE html>'.
					'<html><head><title>403 Forbidden</title><style type="text/css">'.
					'body { padding: 40pt; }'.
					'body, h1, h2, p { color: #333; font-family: Arial, sans-serif; margin: 0; }'.
					'div { width: 200px; background: #eee; padding: 2em; }'.
					'</style></head><body><div><h1>403</h1><h2>Forbidden</h2></div></body></html>';

			$$dataref = $html;
		}
	}
);

1;
