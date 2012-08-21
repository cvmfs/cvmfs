package File::MimeInfo::Rox;

use strict;
use Carp;
use File::BaseDir qw/config_home data_dirs/;
use File::Spec;
require Exporter;

our @ISA = qw(Exporter);
our @EXPORT = qw(mime_exec mime_system);
our @EXPORT_OK = qw(suggest_script_name);
our %EXPORT_TAGS = (magic => \@EXPORT);
our $VERSION = '0.16';
our @choicespath = (
	config_home('rox.sourceforge.net'),
	File::Spec->catdir($ENV{HOME}, 'Choices'),
	data_dirs('Choices'),
);
our ($DEBUG);

sub import {
	my $parent = (grep {$_ eq q/:magic/} @_)
		? q/File::MimeInfo::Magic/
		: q/File::MimeInfo/;
	eval "use $parent";
	die $@ if $@;
	goto \&Exporter::import;
}

sub mime_system { _do_mime('system', @_) }
sub mime_exec   { _do_mime('exec',   @_) }

sub _do_mime {
	my ($act, $file, $mimet) = (shift, shift, shift);

	$mimet ||= mimetype($file);
	return undef unless $mimet;
	print "Using mimetype: $mimet\n" if $DEBUG;

	my $script = _locate_script($mimet);
	return undef unless $script;

	print "Going to $act: $script $file\n" if $DEBUG;
	($act eq 'exec')
		? exec($script, $file, @_)
		: (system($script, $file, @_) == 0)
			or croak "couldn't $act: $script $file";
	42;
}

sub _locate_script {
	my $mime = shift;
	$mime =~ /^(\w+)/;
	my $media = $1;
	$mime =~ s#/#_#;
	my @p = $ENV{CHOICESPATH}
		? split(/:/, $ENV{CHOICESPATH})
		: (@choicespath);
	my $script;
	for (
		map("$_/MIME-types/$mime", @p),
		map("$_/MIME-types/$media", @p)
	) {
		print "looking for: $_\n" if $DEBUG;
		next unless -e $_;
		$script = $_;
		last;
	}
	return undef unless $script;
	$script = "$script/AppRun" if -d $script;
	return -f $script ? $script : undef;
}

sub suggest_script_name {
	my $m = pop;
	$m =~ s#/#_#;
	my @p = $ENV{CHOICESPATH}
		? split(/:/, $ENV{CHOICESPATH})
		: (@choicespath);
	return "$p[0]/MIME-types", $m;
}

1;

__END__

=head1 NAME

File::MimeInfo::Rox - Open files by mimetype "Rox style"

=head1 SYNOPSIS

  use File::MimeInfo::Magic;
  use File::MimeInfo::Rox qw/:magic/;

  # open some file with the apropriate program
  mime_system($somefile);

  # more verbose version
  my $mt = mimetype($somefile)
      || die "Could not find mimetype for $somefile\n";
  mime_system($somefile, $mt)
      || die "No program to open $somefile available\n";


=head1 DESCRIPTION

This module tries to mimic the behaviour of the rox file
browser L<http://rox.sf.net> when "opening" data files.
It determines the mime type and searches in rox's C<Choices>
directories for a program to handle that mimetype.

See the rox documentation for an extensive discussion of this
mechanism.

=head1 EXPORT

The methods C<mime_exec> and C<mime_system> are exported,
if you use the export tag C<:magic> you get the same methods
but L<File::MimeInfo::Magic> will be used for mimetype lookup.

=head1 ENVIRONMENT

The environment variable C<CHOICESPATH> is used when searching
for rox's config dirs. It defaults to
C<$ENV{HOME}/Choices:/usr/local/share/Choices:/usr/share/Choices>

=head1 METHODS

=over 4

=item C<mime_system($file)>

=item C<mime_system($file, $mimetype, @_)>

Try to open C<$file> with the appropriate program for files of
it's mimetype. You can use C<$mimetype> to force the mimetype.
Also if you allready know the mimetype it saves a lot of time
to just tell it.

If either the mimetype couldn't be determinated or
no appropriate program could be found C<undef> is returned.
If the actual L<system> failes an exception is raised.

All remaining arguments are passed on to the handler.

=item C<mime_exec($file)>

=item C<mime_exec($file, $mimetype, @_)>

Like C<mime_system()> but uses L<exec> instead of L<system>,
so it B<never returns> if successful.

=item C<suggest_script_name($mimetype)>

Returns the list C<($dir, $file)> for the suggested place
to write new script files (or symlinks) for mimetype C<$mimetype>.
The suggested dir doesn't need to exist.

=back

=head1 BUGS

Please mail the author when you encounter any bugs.

=head1 AUTHOR

Jaap Karssenberg E<lt>pardus@cpan.orgE<gt>

Copyright (c) 2003, 2012 Jaap G Karssenberg. All rights reserved.
This program is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<File::MimeInfo>,
L<File::MimeInfo::Magic>,
L<http://rox.sourceforce.net>

=cut
