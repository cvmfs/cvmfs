package Functions::Tools;

# Here we'll collect some function wich are shared among other modules but
# don't belong to any one of them in particular

use strict;
use warnings;
use IO::Interface::Simple;

# Next lines are needed to export subroutines to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(get_interface_address);

# This function will accept a network interface and will retrieve the network ip for that interface
sub get_interface_address {
	my $iface = shift;
	my $if = IO::Interface::Simple->new($iface);
	if ($if) {
		return $if->address;
	}
	else {
		return undef;
	}
}

1;
