package HTTP::AppServer::Plugin::JsonCrud;
# Plugin for HTTP::AppServer that provides CRUD methods for
# storing JSON documents in a DBI (relational) database.
# 2010 by Tom Kirchner

################################################################################
#
#		OUT OF DATE
#
################################################################################

#use 5.010000;
use strict;
use warnings;
use Data::UUID;
use DBI;
use JSON;
use HTTP::AppServer::Plugin;
use base qw(HTTP::AppServer::Plugin);

our $VERSION = '0.01';

# database connection settings
my $DBEngine   = 'mysql';
my $DBName     = 'jsondb';
my $DBUsername = 'root';
my $DBPassword = '';
my $DBHost     = 'localhost';

# database settings
my $DBTablePrefix = 'jsondb_';

# UUID generator instance
my $UUIDGenerator = Data::UUID->new();

# called by the server when the plugin is installed
# to determine which routes are handled by the plugin
sub init
{
	my ($class, $server, %options) = @_;

	# analyse options
	$DBEngine			 	= $options{'DBEngine'} 			if exists $options{'DBEngine'};
	$DBName 				= $options{'DBName'} 				if exists $options{'DBName'};
	$DBUsername 		= $options{'DBUsername'} 		if exists $options{'DBUsername'};
	$DBPassword 		= $options{'DBPassword'} 		if exists $options{'DBPassword'};
	$DBHost 				= $options{'DBHost'} 				if exists $options{'DBHost'};
	$DBTablePrefix 	= $options{'DBTablePrefix'} if exists $options{'DBTablePrefix'};

	# install some properties in the server
	$server->set('dbh', undef);
	$server->set('getdbh', \&_get_dbh);
	$server->set('tableprefix', $DBTablePrefix);
	$server->set('uuidgen', $UUIDGenerator);

	$options{'AllowedActions'} = [qw(create read update delete)]
		unless exists $options{'AllowedActions'};

	my %handlers;
	$handlers{'^\/create.*$'} = \&_handle_create
		if scalar grep { $_ eq 'create' } @{$options{'AllowedActions'}};
	$handlers{'^\/read\/([A-FXa-fx0-9]+).*$'} = \&_handle_read
		if scalar grep { $_ eq 'read' } @{$options{'AllowedActions'}};
	$handlers{'^\/update\/([A-FXa-fx0-9]+).*$'} = \&_handle_update
		if scalar grep { $_ eq 'update' } @{$options{'AllowedActions'}};
	$handlers{'^\/delete\/([A-FXa-fx0-9]+).*$'} = \&_handle_delete
		if scalar grep { $_ eq 'delete' } @{$options{'AllowedActions'}};

	return %handlers;

	return (
		# creation of a JSON document
		'^\/create.*$' => \&_handle_create,
		# reading a JSON document
		'^\/read\/([A-FXa-fx0-9]+).*$' => \&_handle_read,
		# updating an existing JSON document
		'^\/update\/([A-FXa-fx0-9]+).*$' => \&_handle_update,
		# deleting a JSON document
		'^\/delete\/([A-FXa-fx0-9]+).*$' => \&_handle_delete,	
	);
}

sub _get_dbh
{
	my ($server) = @_;
	$server->dbh(_connect_db());
		if !defined $server->dbh() || !$server->dbh()->ping();
	return $server->dbh();
}

sub _connect_db
{
	my $dbh =
		DBI->connect("DBI:".$DBEngine.":".$DBName.":".$DBHost, $DBUsername, $DBPassword)
			or die("Could not connect to database: $! $@\n");
	
	# create db tables if nessessary
	my $sth = $dbh->table_info("", $DBName, $DBTablePrefix.'data', "TABLE");
	unless ($sth->fetch()) {
		my $sql = 'create table `'.$DBTablePrefix.'data` (`uuid` varchar(34), `content` text)';
		my $sth = $dbh->prepare($sql);
		$sth->execute() or die "Failed to create tables: $! $@\n";
	}
	
	die "could not connect to database: $! $@\n"
		unless defined $dbh;
	
	return $dbh;
}

sub _handle_create
{
	my ($server, $cgi, @parts) = @_;

	my $json = $cgi->param('data');
	my $uuid = $server->uuidgen()->create_hex();
	my $sql = 'insert into `'.$server->tableprefix().'data` (`uuid`, `content`) values ('.$server->dbh()->quote($uuid).', '.$server->dbh()->quote($json).')';
	my $sth = $server->getdbh($server)->prepare($sql);
		 $sth->execute();

	print $cgi->header('application/json');
	print to_json({'status' => 1, 'info' => 'created object with new uuid', 'uuid' => $uuid});
}

sub _handle_read
{
	my ($server, $cgi, $uuid) = @_;

	my $sql = 'select `content` from `'.$server->tableprefix().'data` where `uuid` = '.$server->dbh()->quote($uuid);
	my $sth = $server->getdbh($server)->prepare($sql);
		 $sth->execute();
	my $result = '{}';to_json({'stmt' => $sql});
	if (my $row = $sth->fetchrow_arrayref()) {
		$result = $row->[0];
	}

	print $cgi->header('application/json');
	print $result;
}

sub _handle_update
{
	my ($server, $cgi, $uuid) = @_;

	my $json = $cgi->param('data');
	my $sql = 'update `'.$server->tableprefix().'data` set `content` = '.$server->dbh()->quote($json).' where `uuid` = '.$server->dbh()->quote($uuid);
	my $sth = $server->getdbh($server)->prepare($sql);
		 $sth->execute();

	print $cgi->header('application/json');
	print to_json({'status' => 1, 'info' => 'updated object', 'uuid' => $uuid});
}

sub _handle_delete
{
	my ($server, $cgi, $uuid) = @_;

	my $sql = 'delete from `'.$server->tableprefix().'data` where `uuid` = '.$server->dbh()->quote($uuid);
	my $sth = $server->getdbh($server)->prepare($sql);
		 $sth->execute();

	print $cgi->header('application/json');
	print to_json({'status' => 1, 'info' => 'deleted object', 'uuid' => $uuid});
}

1;
__END__
=head1 NAME

HTTP::AppServer - Perl extension for blah blah blah

=head1 SYNOPSIS

  use HTTP::AppServer;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for HTTP::AppServer, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Tom Kirchner, E<lt>tk@apple.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 by Tom Kirchner

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
