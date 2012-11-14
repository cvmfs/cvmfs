package HTTP::AppServer::Base;
# Simple HTTP server that uses regular expressions
# to disptach URLs to Perl function references that contain the logic.
# 2010 by Tom Kirchner

#use 5.010000;
use strict;
use warnings;
use Data::Dumper;
use HTTP::Server::Simple::CGI;
use base qw(HTTP::Server::Simple::CGI);

our $VERSION = '0.01';

# defines a handler (function reference) for each dispatching handler
my $Handlers = [
	# {'regex' => <regex>, 'handler' => <coderef> or <string>}
];

sub set
{
	my ($self, $name, $value) = @_;
	if (exists $self->{'sets'}->{$name}) {
		die "Cannot set key '$name' more than once.\n";
	} else {
		$self->{'sets'}->{$name} = $value;
		
		# store name of plugin
		my $caller = caller();
		   $caller =~ s/.*\://g;
		$self->{'sets-plugins'}->{$name} = $caller;
	}
	return $self;
}

sub AUTOLOAD
{
	my ($self, @args) = @_;

	my $name = $HTTP::AppServer::Base::AUTOLOAD;
	   $name =~ s/.*\://g;
	
	if (exists $self->{'sets'}->{$name}) {
		my $value = $self->{'sets'}->{$name};
		if (ref $value eq 'CODE') {
			# call method
			return $value->($self, @args);
		}
		else {
			if (scalar @args > 0) {
				# set	property
				$self->{'sets'}->{$name} = $args[0];
			}
			# get property
			return $self->{'sets'}->{$name};
		}
	}
	else {
		eval('return SUPER::'.$name.'(@args)');
	}
	#else {
	#	print STDERR "Access to unknown property or method '$name'\n";
	#	return ();
	#}
}

sub debug
{
	my ($self) = @_;
	print "\nroutes:\n";
	map { print "  ".$_->{'regex'}."\n" } @{$Handlers};
	print "\nproperties and methods:\n";
	map { print "  ".sprintf('%-16s',$_)." (plugin ".$self->{'sets-plugins'}->{$_}.")\n" } sort keys %{$self->{'sets'}};
	print "\n";
}

sub handle
{
	my ($self, $regex, $handler) = @_;
	push @{$Handlers}, {'regex' => $regex, 'handler' => $handler};
	return $self;
}

sub handle_request
{
	my ($self, $cgi) = @_;
	unless ($self->_dispatch_path($cgi, $cgi->path_info(), {})) {
		# return an error
		$self->errorpage();
		return 0;
	}
	return 1;
}

sub _dispatch_path
{
	my ($self, $cgi, $path, $called) = @_;

	# try to find dispatch for current request
	my $i = 0;
	foreach my $h (@{$Handlers}) {
		my $regex   = $h->{'regex'};
		my $handler = $h->{'handler'};
		#print STDERR 
		#	"[$i] $regex ($path) ".
		#	"(called? ".(exists $called->{"$i"} == 1 ? '1':'0').") ".
		#	"(match? ".($path =~ /$regex/ == 1 ? '1':'0').")\n";
		if (!exists $called->{"$i"} && $path =~ /$regex/) {
			my (@parts) = $path =~ /$regex/;
			if (ref $handler eq 'CODE') {
				# handler is a function
				my $result = $handler->( $self, $cgi, @parts );
				$result = 1 unless defined $result;
				#print STDERR "  => $result\n";
				$called->{"$i"} = 1;
				if ($result == 0) {
					# go on searching
					#print STDERR "  => go on\n";
					return $self->_dispatch_path($cgi, $path, $called);
				} else {
					# stop here
					#print STDERR "  => stop\n";
					return 1;
				}
			}
			else {
				# handler forwards to other handler
				return $self->_dispatch_path($cgi, $handler, $called);
			}
		}
		$i++;
	}
	return 0;
}

1;
