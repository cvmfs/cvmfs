use strict;
use warnings;

###########
# MODULES #
###########

use FindBin qw($RealBin);
use lib $RealBin;

use Tests::Common qw(setup_environment set_stdout_stderr open_test_socket close_test_socket open_shellout_socket);

use ZeroMQ qw/:all/;


############################
# TEST NAME  AND REPO NAME #
############################

my $testname = 'DUMMY';

########################
# COMMAND LINE OPTIONS #
########################

use Getopt::Long;

my $outputfile = '/var/log/cvmfs-test/dummy.out';
my $errorfile = '/var/log/cvmfs-test/dummy.err';
my $no_clean = undef;

my $ret = GetOptions ( "stdout=s" => \$outputfile,
					   "stderr=s" => \$errorfile,
					   "no-clean" => \$no_clean );
###########					   
# FORKING #
###########

my $pid = fork();

###################################
# COMMANDS FOR THE FORKED PROCESS #
###################################

if (defined($pid) and $pid == 0) {
	set_stdout_stderr($outputfile, $errorfile);
	my ($shell_socket, $shell_ctxt) = open_shellout_socket();
	
	print "Dummy test in progress";
	
	$shell_socket->send("Dummy Test OK.\n");
	$shell_socket->send("END\n");	
	
	close_test_socket($shell_socket, $shell_ctxt);
}

#################################
# COMMANDS FOR THE MAIN PROCESS #
#################################

if (defined($pid) and $pid != 0) {
	print "$testname test started.\n";
	print "You can read its output in $outputfile.\n";
	print "Errors are stored in $errorfile.\n";
	
	print "PROCESSING:$testname\n";
	
	print "READ_RETURN_CODE";
}

exit 0;
