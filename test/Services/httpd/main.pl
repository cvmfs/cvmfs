use strict;
use warnings;
use FindBin qw($RealBin);
use lib "$RealBin";
use HTTP::AppServer;
use Getopt::Long;

my $not_found = undef;
my $forbidden = undef;
my $serve_all = undef;
my $index_of = undef;
my $timeout = undef;
my $deliver_crap = undef;
my $serve_error = 0;
my $port = 8080;
my $docroot = '/tmp';
my $retriever = 'FileRetriever';
my $outputfile = '/var/log/cvmfs-test/httpd.out';
my $errorfile = '/var/log/cvmfs-test/httpd.err';

my $ret = GetOptions ( "404" => \$not_found,
					   "403" => \$forbidden,
					   "all" => \$serve_all,
					   "timeout" => \$timeout,
					   "index-of" => \$index_of,
					   "port=i" => \$port,
					   "root=s" => \$docroot,
					   "stdout=s" => \$outputfile,
					   "stderr=s" => \$errorfile,
					   "deliver-crap" => \$deliver_crap );

if (defined ($serve_all)) {
	$retriever = 'AllRetriever';
}

if (defined ($not_found)){
	$retriever = 'Retriever404';
	$serve_error = 1;
}

if (defined ($forbidden)) {
	$retriever = 'Retriever403';
	$serve_error = 1;
}

if (defined ($timeout)) {
	$retriever = 'Timeout';
}

if (defined ($deliver_crap)) {
	$retriever = 'CrapDeliver';
}

# Create server instance at localhost:$port
my $server = HTTP::AppServer->new( StartBackground => 0, ServerPort => $port );
 
# Alias URL
if (!defined ($index_of) or $serve_error == 1) {
	$server->handle('^\/$', '/index.html');
}
else {
	$server->plugin("IndexOf");
}

# Loading requested plugin
$server->plugin("$retriever", DocRoot => "$docroot");

# Loading plugin for error handling
$server->plugin('CustomError');

my $pid = fork;

# Command for the forked process
if (defined ($pid) and $pid == 0){
	open (my $errfh, '>', $errorfile) || die "Couldn't open $errorfile: $!\n";
	STDERR->fdopen ( \*$errfh, 'w' ) || die "Couldn't set STDERR to $errorfile: $!\n";
	STDERR->autoflush;
	open (my $outfh, '>', $outputfile) || die "Couldn't open $outputfile: $!\n";
	STDOUT->fdopen( \*$outfh, 'w' ) || die "Couldn't set STDOUT to $outputfile: $!\n";
	STDOUT->autoflush;
	# Starting server in the forked process
	$server->start;
}

# Command for the main script
unless ($pid == 0) {
	print "HTTPd started on port $port with PID $pid.\n";
	print "It's serving the folder $docroot.\n";
	print "You can read its output in '$outputfile'.\n";
	print "Errors are stored in '$errorfile'.\n";
	print "SAVE_PID:$pid\n";
}

exit 0;
