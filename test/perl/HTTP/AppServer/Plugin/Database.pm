package HTTP::AppServer::Plugin::Database;
# Plugin for HTTP::AppServer that provides a connection
# to a DBI database handle (relational database).
# On top of that it provides simplified calling of
# common database queries, such as select, update, delete etc.
# 2010 by Tom Kirchner

#use 5.010000;
use strict;
use warnings;
use DBI;
use Data::Dumper;
use HTTP::AppServer::Plugin;
use base qw(HTTP::AppServer::Plugin);

our $VERSION = '0.01';

# database connection settings
my $DBEngine   = 'mysql';
my $DBName     = 'jsondb';
my $DBUsername = 'root';
my $DBPassword = '';
my $DBHost     = 'localhost';

# the database schema directory: <path>/<version>-(up|down).sql
my $SchemaDir = '';

# latest version of database to use
my $UseVersion = '';

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
	$SchemaDir 			= $options{'SchemaDir'} 		if exists $options{'SchemaDir'};
	$UseVersion 		= $options{'UseVersion'} 		if exists $options{'UseVersion'};

	# install some properties in the server
	$server->set('dbh', undef);
	$server->set('getdbh', \&_get_dbh);

	# install some methods that perform database queries
	$server->set('find',   \&_db_find);
	$server->set('findall',\&_db_findall);
	$server->set('create', \&_db_create);
	$server->set('update', \&_db_update);
	$server->set('remove', \&_db_remove);
	$server->set('load',   \&_db_load);
	$server->set('query',  \&_db_query);
	
	$server->set('_make_sql_where_clause', \&_make_sql_where_clause);
	$server->set('_quote', \&_quote);
	$server->set('_quotename', \&_quotename);
	$server->set('_parse_params', \&_parse_params);
	$server->set('_load_data_file', \&_load_data_file);

	if (length $SchemaDir && length $UseVersion) {
		# install meta table with info on database version
		_init_database_meta($server);
		
		# try to migrate database to the used version
		_migrate_database($server);
		
		#print "\nBUGGY -> see Plugin/Database.pm!!!!\n";
		#exit;
	}

	# this plugin does not install any URL handlers
	return ();
}

sub _get_dbh
{
	my ($server) = @_;
	$server->dbh(_connect_db())
		if !defined $server->dbh() || !$server->dbh()->ping();
	return $server->dbh();
}

sub _connect_db
{
	my $dbh =
		DBI->connect(
				"DBI:".$DBEngine.":".$DBName.":".$DBHost, $DBUsername, $DBPassword,
				{ PrintError => 0 },
			)
			or _die("Database: Could not connect to database: $! $@\n");	
	_die("could not connect to database: $! $@\n")
		unless defined $dbh;
	return $dbh;
}

sub _init_database_meta
{
	my ($server) = @_;

	# create db tables if nessessary
	my $tablename = 'meta';
	my $sth = $server->getdbh()->table_info("", $DBName, $tablename, "TABLE");
	unless ($sth->fetch()) {
		my $sql = 'create table `'.$tablename.'` (`version` varchar(255))';
		my $sth = $server->getdbh()->prepare($sql);
		$sth->execute() or _die("Failed to create tables: $! $@\n");
		
		# set current version to 1.00 (the first version)
		$server->create(
			-table => 'meta',
			-row   => {'version' => '0.00'},
		);
	}
}

sub _migrate_database
{
	my ($server) = @_;
	
	# fetch current version
	my $qu = $server->find(-tables => ['meta'], -limit => 1);
	if (my $version = $qu->fetchrow_hashref()) {
		$version = $version->{'version'};
		
		# tests
		#foreach my $pair (['0.00','3.00'], ['1.00','3.00'], ['2.01','3.00'],
		#									['1.00','3.00'], ['2.01','3.00'], ['3.00','3.00'],
		#									['3.00','0.00'], ['3.00','1.00'], ['3.00','2.01'],
		#									['2.01','0.00'], ['2.01','1.00'], ['2.01','0.00'],
		#									['2.01','2.01'], ['1.00','2.01']) { 
		#$version = $pair->[0];
		#$UseVersion = $pair->[1];
		#print STDERR "------------------------------------------------ $version => $UseVersion\n";
		
		map { _exec_sql_file($server, $_) } _get_migrations($version, $UseVersion);
		
		#}
		#exit;
		
		# update version in database
		$server->update(
			-table => 'meta',
			-set   => {'version' => $UseVersion},
		);
		print STDERR "Database: db now at version $UseVersion\n";
	}
}

sub _exec_sql_file
{
	my ($server, $filename) = @_;
	print STDERR "Database: executing sql file '$filename'\n";

	open(FH, '<'.$filename) or _die("Database: failed to open sql file '$filename': $! $@\n");
	my $sql = join '', <FH>;
	close FH;
	
	foreach my $s (split /\;\r?\n/, $sql) {
		$server->query($s, 1);
	}
}

sub _get_migrations
{
	my ($from, $to) = @_;
	return () if $from eq $to;
	
	# get defined migrations and direction
	my $migrations = _load_migrations($SchemaDir);
	my $dir = ($from ge $to ? 'down' : 'up');
	my @versions = sort keys %{$migrations};

	$migrations->{'0.00'} = {'version' => '0.00'};
	unshift @versions, '0.00';
	
	#print STDERR Dumper($migrations);
	#print STDERR Dumper(\@versions);

	die "Database: Failed to migrate from non-existant version '$from'\n"
		unless exists $migrations->{$from};
	die "Database: Failed to migrate to non-existant version '$to'\n"
		unless exists $migrations->{$to};

	# find index of version in list of all versions
	my $i = 0;
	foreach my $v (@versions) {
		if ($v eq $from) {
			last;
		}
		$i++;
	}
	#print STDERR Dumper(\@versions);
	#print STDERR "i = $i\n";

	# collect all sql files from $from to $to version
	my @files = ();
	my @files_v = ();
	for (my $j = $i;
	     ($dir eq 'up' ? ($j <= $#versions && $j <= $to) : ($j >= 0 && $j >= $to)); 
	     $j += ($dir eq 'up' ? 1 : -1)) {

		if (exists $migrations->{$versions[$j]}->{$dir}) {		
			push @files, $migrations->{$versions[$j]}->{$dir};
			push @files_v, $versions[$j];
		}
	}
	
	shift @files
		if $dir eq 'up' && $from ne '0.00';
	pop @files
		if $dir eq 'down' && $to ne '0.00';
	unshift @files, $migrations->{$from}->{$dir}
		if $dir eq 'down' && exists $migrations->{$from}->{$dir} &&
			 (!scalar @files || $files[0] ne $migrations->{$from}->{$dir});

	return @files;
}

sub _load_migrations
{
	my ($dir) = @_;
	opendir(DIR, $dir) 
		or _die("Database: Failed to load schema directory '$dir': $! $@\n");
	my $migrations = {};
	foreach my $entry (sort readdir DIR) {
		my $file = $dir.'/'.$entry;
		my $regex = '^(.*)\-(up|down)\.sql$';
		if (-f $file && -r $file && $file =~ /$regex/) {
			my ($version, $action, $ending) = $entry =~ /$regex/;
			$migrations->{$version} = {'version' => $version}
				unless exists $migrations->{$version};
			$migrations->{$version}->{$action} = $file;
		}
	}
	closedir DIR;
	return $migrations;
}

# ------------------------------------------------------------------------------

sub _db_findall
{
	my ($server, @args) = @_;
	my $qu = $server->find(@args);
	my @rows = ();
	while (my $row = $qu->fetchrow_hashref()) {
		push @rows, $row;
	}
	return @rows;
}

sub _db_find
{
	my ($server, %options) = @_;
	my $opts = $server->_parse_params( \%options,
		{
			tables 		=> [],
			where 		=> {},
			wherelike => {},
			group 		=> [],
			order 		=> [],
			limit 		=> 0,
			distinct 	=> 0,
			columns		=> [],
			joins		  => {},
			sortdir		=> 'asc', # 'asc' or 'desc'
		});

	my @tables = map { $server->_quotename($_) } @{$opts->{'tables'}};

	my @columns = map { $server->_quotename($_) } @{$opts->{'columns'}};

	my @joins =
		map {
			$server->_quotename($_).' = '.$server->_quotename($opts->{'joins'}->{$_});
		}
		keys %{$opts->{'joins'}};

	my @group = map { $server->_quotename($_) } @{$opts->{'group'}};

	my @order = map { $server->_quotename($_) } @{$opts->{'order'}};
	
	my $sql
		= 'SELECT'
		.(defined $opts->{'distinct'} ? ' DISTINCT' : '')
		.' '.(scalar @columns ? join(', ', @columns) : '*')
		.' FROM '.join(', ', @tables)
		.' WHERE '
		.(scalar keys %{$opts->{'where'}} ?
			$server->_make_sql_where_clause($opts->{'where'})
			: '1')
		.(scalar keys %{$opts->{'wherelike'}} ?
			' AND '.$server->_make_sql_where_clause($opts->{'wherelike'}, 1)
			: '')
		.(scalar @joins ? ' AND '.join(' AND ', @joins) : '')
		.(scalar @group ? ' GROUP BY '.join(', ', @group) : '')
		.(scalar @order ? ' ORDER BY '.join(', ', @order).' '.uc($opts->{'sortdir'}) : '')
		.($opts->{'limit'} > 0 ? ' LIMIT '.$opts->{'limit'} : '');
	
	return $server->query($sql);
}

sub _db_create
{
	my ($server, %options) = @_;
	my $opts = $server->_parse_params( \%options,
		{
			table => undef,
			row => {},
		});

	my @columns;
	my @values;
	map {
		push @columns, $server->_quotename($_);
		push @values,  $server->_quote($opts->{'row'}->{$_});
	}
	keys %{$opts->{'row'}};

	my $sql
		= 'INSERT'
			.' INTO '.$server->_quotename($opts->{'table'})
			.' ('.join(', ', @columns).')'
			.' VALUES ('.join(', ', @values).')';

	$server->query($sql);
	return $server->getdbh()->last_insert_id(undef, undef, $opts->{'table'}, 'id');
}

sub _db_update
{
	my ($server, %options) = @_;
	my $opts = $server->_parse_params( \%options,
		{
			table => '',
			set => {},
			where => {},
			wherelike => {},
		});

	my @sets =
		map {
			$server->_quotename($_).' = '.$server->_quote($opts->{'set'}->{$_});
		}
		keys %{$opts->{'set'}};

	my $sql
		= 'UPDATE'
			.' '.$server->_quotename($opts->{'table'})
			.' SET '.join(', ', @sets)
			.' WHERE '
			.(scalar keys %{$opts->{'where'}} ?
				$server->_make_sql_where_clause($opts->{'where'})
				: '1')
			.(scalar keys %{$opts->{'wherelike'}} ?
				' AND '.$server->_make_sql_where_clause($opts->{'wherelike'}, 1)
				: '');

	return $server->query($sql);
}

sub _db_remove
{
	my ($server, %options) = @_;
	my $opts = server->_parse_params( \%options,
		{
			table => '',
			where => {},
			wherelike => {},
		});

	my $sql
		= 'DELETE'
			.' FROM '.$server->_quotename($opts->{'table'})
			.' WHERE '
			.(scalar keys %{$opts->{'where'}} ?
				$server->_make_sql_where_clause($opts->{'where'})
				: '1')
			.(scalar keys %{$opts->{'wherelike'}} ?
				' AND '.$server->_make_sql_where_clause($opts->{'wherelike'}, 1)
				: '');

	return $server->query($sql);
}

sub _db_load
{
	my ($server, $filename, $tablename) = __parse_args(@_);
	
	my $records	= server->_load_data_file($filename);
	
	my $inserted = 0;
	foreach my $record (@{$records}) {
		_die("record does not have an id field, in data file '$filename'")
			unless exists $record->{'id'};
		
		my $query
			= $server->find(
				-tables => [$tablename],
				-where  => {'id' => $record->{'id'}},
				-limit  => 1,
			);
			
		if (my $row = $query->fetchrow_hashref()) {
			# do nothing
		}
		else {
			# insert
			$server->create(
				-table => $tablename,
				-row   => $record,
			);
			$inserted ++;	
		}
	}
	return (scalar @{$records}, $inserted);
}

sub _db_query
{
	my ($server, $sql, $ignore_errors) = @_;
	$ignore_errors = 0 unless defined $ignore_errors;
	
	my $dbh = $server->getdbh();
	my $query = $dbh->prepare($sql);
	my $result = $query->execute();
	_die('the query ['.$sql.'] failed: '.DBI->errstr())
		if !$result && !$ignore_errors;
	return $query;
}

sub _die
{
	my ($msg) = @_;
	print STDERR "Database: $msg\n";
}

# ------------------------------------------------------------------------------

sub _load_data_file
{
	my ($server, $datafilename) = @_;
	
	if (-f $datafilename && -r $datafilename) {
		open DATAFILE, '<'.$datafilename
			or _die("failed to open file '$datafilename': $!");
			
		my @records;
		my $current_id     = undef;
		my $current_field  = undef;
		my $current_record = {};
		foreach my $line (<DATAFILE>) {
					
			if (defined $current_id && defined $current_field && $line =~ /^[\s\t]/) {
				# possibly field value line
				$line =~ s/^[\s\t]//;
				$current_record->{$current_field} .= $line;
			}
			else {
				if ($line =~ /^\[(\d+)\][\s\t\n\r]*$/) {
					# id line
					if (defined $current_id) {
						# save previous record
						push @records, $current_record;
					}
					# reset
					$current_id = $line;
					$current_id =~ s/^\[(\d+)\][\s\t\n\r]*$/$1/;
					$current_record = { 'id' => $current_id };
				}
				elsif ($line =~ /^(\w+)[\s\t]*([\:\.])(.*)\n\r?$/) {
					# field line
					my ($fieldname, $type, $value)
						= $line =~ /^(\w+)[\s\t]*([\:\.])(.*)\n\r?$/;
					if ($type eq ':') {
						$current_record->{$fieldname} = $value;
						$current_field = undef;
					}
					else {
						$current_record->{$fieldname} = '';
						$current_field = $fieldname;
					}
				}
			}
		}
		if (defined $current_id) {
			# save last record
			push @records, $current_record;
		}		
		return \@records;
	}
	else {
		_die("failed to open file '$datafilename': no file or not readable");
	}
}

sub _make_sql_where_clause
{
	my ($server, $where, $use_like) = @_;
	$use_like = 0 unless defined $use_like;
	
	my @parts =
		map {
			my $fieldname  = $server->_quotename($_);
			my $fieldvalue = (defined $where->{$_} ? $server->_quote($where->{$_}) : 'NULL');
			
			my $s  = $fieldname;
			   $s .= ($use_like == 1 ? ' LIKE ' : (defined $where->{$_} ? ' = ' : ' IS '));
			   $s .= ''.$fieldvalue;
			$s;
		}
		keys %{$where};
	
	return join(' AND ', @parts);
}

sub _quote
{
	my ($server, @args) = @_;
	my $dbh = $server->getdbh();
	
	return $dbh->quote(@args)
		or _die('quote failed: '.DBI->errstr());
}

# escapes a CGI::WebToolkit field identifier, e.g. "mytable.myfield" or "myfield" etc.
sub _quotename
{
	my ($server, $fieldname) = @_;
	my $dbh = $server->getdbh();

	my @parts = split /\./, $fieldname;
	
	my $quoted;
	if (scalar @parts == 1) {
		$quoted = '`'.$parts[0].'`';
	} else {
		$quoted = '`'.$parts[0].'`'.'.'.'`'.$parts[1].'`';
	}
	return $quoted;
}

sub _parse_params
{
	my ($server, $params, $defaults) = @_;
	my $values = {};
	foreach my $key (keys %{$defaults}) {
		$values->{$key} = $defaults->{$key};
	}
	foreach my $key (keys %{$params}) {
		my $cleankey = lc $key;
		   $cleankey =~ s/^\-//;
		$values->{$cleankey} = $params->{$key}
			if exists $defaults->{$cleankey};
	}
	return $values;
}

1;
__END__
=head1 NAME

HTTP::AppServer::Plugin::Database - Plugin for HTTP::AppServer that allows easy DBI database access.

=head1 SYNOPSIS

  use HTTP::AppServer;
  my $server = HTTP::AppServer->new();
  $server->plugin('Database',
    DBEngine   => 'mysql',
    DBName     => 'ui',
    DBUsername => 'root',
    DBPassword => '',
    DBHost     => 'localhost',
    SchemaDir  => './db',
    UseVersion => '4.00',
  );

=head1 DESCRIPTION

Plugin for HTTP::AppServer that provides a connection
to a DBI database handle (relational database).
On top of that it provides simplified calling of
common database queries, such as select, update, delete etc.

=head2 Plugin configuration

=head3 DBEngine => I<name>

The database engine to use, e.g. 'mysql'. This is a valid DBI engine name.

=head3 DBName => I<name>

The database name.

=head3 DBUsername => I<name>

The username of the database user.

=head3 DBPassword => I<password>

The password of the database user.

=head3 DBHost => I<host>

The host of the database, default is "localhost".

=head3 SchemaDir => I<path>

This plugin allows for automatic migration of database schemas.
The schemas are stored in the SchemaDir directory in this form (example):

  /1.00-up.sql
  /1.00-down.sql
  /1.01-up.sql
  /1.01-down.sql
  /2.00-up.sql
  /2.00-down.sql
  /...

Versions should start at 1.00.
In this example, version 1.00 is created by executing 1.00-up.sql
and undone by executing 1.00-down.sql.

=head3 UseVersion => I<version>

When loaded, this plugin migrates the database to the schema of
that version. That means all *.sql files in SchemaDir that lie
on the path from the current database version to the UseVersion version
are beeing executed.

The implicit version '0.00' can be used to create a state for
the database where all migrations are undone.

=head2 Installed URL handlers

None.

=head2 Installed server properties

None.

=head2 Installed server methods

=head3 find()

To retrieve records from the database, use the select() method:

  my $query = $server->find(
    -tables   => [qw(mytable1 mytable2 ...)],
    -where    => { name => "...", ... },
    -wherelike  => {...},
    -group    => [qw(id name ...)],
    -order    => [qw(id name ...)],
    -limit    => 10,
    -distinct   => 1,
    -columns    => [qw(id name ...)],
    -joins    => { name => name, ... },
    -sortdir    => 'asc', # or 'desc'
  );

To access the records of the result set, use the normal DBI methods:

  my $array = $query->fetchrow_arrayref();
  my $hash = $query->fetchrow_hashref();
  while (my $record = $query->fetchrow_arrayref()) {
    # ...
  }
  # ...

=head3 findall()

Same as find() but returns not a result but all entries matching 
as plain Perl array.

=head3 create()

To insert a record, use the create() method:

  my $id = $server->create(
    -table => "...",
    -row => { name => "...", ... },
  );

=head3 update()

To update fields in a record, use the update() method:

  my $success = $server->update(
    -table => "...",
    -set => { name => "...", ... },
    -where => { ... },
    -wherelike => { ... },
  );

=head3 remove()

To delete records, use the remove() method:

  my $query = $server->remove(
    -table => "...",
    -where => { ... },
    -wherelike => { ... },      
  );

=head3 query()

Any kind of other query can be executed using the query() method:

  my $query = $server->query( $sql );

=head3 load()

This method is used to import a text file that contains a number of
records into a certain table in the database. This is nice, if you
set up many databases for an application and want to insert some
default data all at once.

Example:

  load( 'my_project', 'default_data', 'my_table' );

This example will load the file I<data/my_project/default_data.txt>
from the configured I<private> directory into the database
table named "my_table" (in the configured database).

The data file must be in a certain format. Here is an example:

  [1]
  name:Mr.X
  age:23

  [2]
  name:Mr.Y
  age:56
  bio.
    Born in 1980, Mr.Y
    was the first to invent
    the toaster.

  [3]
  name:Mrs.Y
  age:25

Each data file can contain zero or more records, each of which
starts with a line containing the id of the record in brackets.
Each line after that contains a field value, which starts with the
field name followed by a colon (":"), followed by the field value
up to the end of line (without the newline).

If a field value contains newlines, the fieldname must be followed
by a dot (instead of a colon) and the following lines are considered
the value of the field. The field value lines must contain a
space character (space or horizontal tab) at the line start,
which identifies them as field value lines but is ignored.

Due to the format, certain restrictions apply to data that
is stored in data files:

=over 1

=item 1 The table must have a column named I<id>.

=item 2 Field names are not allowed to contain colons, dots
or newline characters.

=back

When inserted in the database, CGI::WebToolkit checks first, if a certain row
with that id already exists. If so, nothing happens. In almost all
cases you do not want to have your data in the database be overwritten
by data from data files.

Empty lines in data files and space characters before the colon
are completely ignored.

=head1 SEE ALSO

HTTP::AppServer, HTTP::AppServer::Plugin

=head1 AUTHOR

Tom Kirchner, E<lt>tom@tkirchner.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 by Tom Kirchner

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
