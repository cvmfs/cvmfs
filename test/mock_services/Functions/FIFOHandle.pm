package Functions::FIFOHandle;

#########################################
# Here will be stored all FIFO related functions
#########################################

use strict;
use warnings;
use IO::Handle;
use POSIX qw(mkfifo);

# Next line are needed to export subroutines to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(open_rfifo open_wfifo close_fifo print_to_fifo make_fifo unlink_fifo);

# This function will create a new FIFO
sub make_fifo {
    my $fifo = shift;
    if (-e $fifo) {
		unlink($fifo);
    }
    mkfifo($fifo, 0777) || die "Couldn't create $fifo: $!\n";
    system("chmod 777 $fifo");
}

# This function will simply unlink any FIFO.
sub unlink_fifo {
    my $fifo = shift;
    unlink($fifo);
}

# This function will open the FIFO for reading and will return his handler
sub open_rfifo {
	# Retrieving argument: file path
	my $fifo = shift;
	
	# Opening the FIFO
	open (my $fh, '<', $fifo) || die "Couldn't open $fifo: $!\n";
	
	return $fh;
}

# This function will open the FIFO for writing and will return his handler
sub open_wfifo {
	# Retrieving argument: file path
	my $fifo = shift;
	
	# Opening the FIFO
	open (my $fh, '>', $fifo) || die "Couldn't open $fifo: $!\n";

	return $fh;
}

# This function will close the given FIFO, actually it's just a rename for
# the close function to remind that we are working with FIFO
sub close_fifo {
	# Retrieving argument: file handler
	my $fh = shift;
	
	close $fh;
}

# This function will open the FIFO, print on it or die if something goes wrong
sub print_to_fifo {
	# Retrieving arguments: the FIFO path, the line to print and options
	my $fifo = shift;
	my $line = shift;
	my $options = shift;
	
	# Opening the FIFO
	my $fh = open_wfifo($fifo) || die "Couldn't open $fifo: $!\n";

	# Printing to FIFO
	print $fh $line || die "Couldn't write: $!\n";
	
	if (defined ($options) and $options eq "SNDMORE\n") {
		print $fh "SNDMORE\n";
	}
	# Closing the FIFO
	close_fifo($fh);
}

1;
