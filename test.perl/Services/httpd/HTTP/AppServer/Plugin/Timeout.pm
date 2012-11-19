package HTTP::AppServer::Plugin::Timeout;

#use 5.010000;
use strict;
use warnings;
use IO::File;
use JSON;
use Path::Trim;
use HTTP::AppServer::Plugin;
use base qw(HTTP::AppServer::Plugin);

our $VERSION = '0.01';

# document root for file retrieval
my $DocRoot = '/tmp';

my $PathTrimmer = Path::Trim->new();

# called by the server when the plugin is installed
# to determine which routes are handled by the plugin
sub init
{
	my ($class, $server, %options) = @_;

	$PathTrimmer->set_directory_separator('/');

	# analyse options
	$DocRoot = $options{'DocRoot'} if exists $options{'DocRoot'};

	# install properties in server
	$server->set('docroot', $DocRoot);

	return (
		# handle file (and directory) requests
		'^\/(.*)$' => \&_handle_file,
	);
}

sub _handle_file
{
	sleep 300;
}

1;
