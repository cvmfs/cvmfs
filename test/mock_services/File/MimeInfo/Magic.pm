
package File::MimeInfo::Magic;

use strict;
use Carp;
use Fcntl 'SEEK_SET';
use File::BaseDir qw/data_files/;
require File::MimeInfo;
require Exporter;

BEGIN {
	no strict "refs";
	for (qw/extensions describe globs inodetype default/) {
		*{$_} = \&{"File::MimeInfo::$_"};
	}
}

our @ISA = qw(Exporter File::MimeInfo);
our @EXPORT = qw(mimetype);
our @EXPORT_OK = qw(extensions describe globs inodetype magic);
our $VERSION = '0.16';
our $DEBUG;

our $_hashed = 0;
our (@magic_80, @magic, $max_buffer);
# @magic_80 and @magic are used to store the parse tree of magic data
# @magic_80 contains magic rules with priority 80 and higher, @magic the rest
# $max_buffer contains the maximum number of chars to be buffered from a non-seekable
# filehandle in order to do magic mimetyping

sub mimetype {
	my $file = pop;
	croak 'subroutine "mimetype" needs a filename as argument' unless defined $file;

	return magic($file) || default($file) if ref $file;
	return &File::MimeInfo::mimetype($file) unless -s $file and -r _;

	my ($mimet, $fh);
	return $mimet if $mimet = inodetype($file);

	($mimet, $fh) = _magic($file, \@magic_80); # high priority rules
	return $mimet if $mimet;

	return $mimet if $mimet = globs($file);

	($mimet, $fh) = _magic($fh, \@magic); # lower priority rules
	close $fh unless ref $file;

	return $mimet if $mimet;
	return default($file);
}

sub magic {
	my $file = pop;
	croak 'subroutine "magic" needs a filename as argument' unless defined $file;
	return undef unless ref($file) || -s $file;
	print STDERR "> Checking all magic rules\n" if $DEBUG;

	my ($mimet, $fh) = _magic($file, \@magic_80, \@magic);
	close $fh unless ref $file;

	return $mimet;
}

sub _magic {
	my ($file, @rules) = @_;
	_rehash() unless $_hashed;

	my $fh;
	unless (ref $file) {
		open $fh, '<', $file or return undef;
		binmode $fh;
	}
	else { $fh = $file }

	for my $type (map @$_, @rules) {
		for (2..$#$type) {
			next unless _check_rule($$type[$_], $fh, 0);
			close $fh unless ref $file;
			return ($$type[1], $fh);
		}
	}
	return (undef, $fh);
}

sub _check_rule {
	my ($ref, $fh, $lev) = @_;
	my $line;

	# Read
	if (ref $fh eq 'GLOB') {
		seek($fh, $$ref[0], SEEK_SET);	# seek offset
		read($fh, $line, $$ref[1]);	# read max length
	}
	else { # allowing for IO::Something
		$fh->seek($$ref[0], SEEK_SET);	# seek offset
		$fh->read($line, $$ref[1]);	# read max length
	}

	# Match regex
	$line = unpack 'b*', $line if $$ref[2];	# unpack to bits if using mask
	return undef unless $line =~ $$ref[3];	# match regex
	print STDERR	'>', '>'x$lev, ' Value "', _escape_bytes($2),
			'" at offset ', $$ref[1]+length($1),
			" matches at $$ref[4]\n"
		if $DEBUG;
	return 1 unless $#$ref > 4;

	# Check nested rules and recurs
	for (5..$#$ref) {
		return 1 if _check_rule($$ref[$_], $fh, $lev+1);
	}
	print STDERR "> Failed nested rules\n" if $DEBUG && ! $lev;
	return 0;
}

sub rehash {
	&File::MimeInfo::rehash();
	&_rehash();
	#use Data::Dumper;
	#print Dumper \@magic_80, \@magic;
}

sub _rehash {
	local $_; # limit scope of $_ ... :S
	($max_buffer, @magic_80, @magic) = (32); # clear data
	my @magicfiles = @File::MimeInfo::DIRS
		? ( grep {-e $_ && -r $_}
			map "$_/magic", @File::MimeInfo::DIRS )
		: ( reverse data_files('mime/magic') ) ;
	my @done;
	for my $file (@magicfiles) {
		next if grep {$file eq $_} @done;
		_hash_magic($file);
		push @done, $file;
	}
	@magic = sort {$$b[0] <=> $$a[0]} @magic;
	while ($magic[0][0] >= 80) {
		push @magic_80, shift @magic;
	}
	$_hashed = 1;
}

sub _hash_magic {
	my $file = shift;

	open MAGIC, '<', $file
		|| croak "Could not open file '$file' for reading";
	binmode MAGIC;
	<MAGIC> eq "MIME-Magic\x00\n"
		or carp "Magic file '$file' doesn't seem to be a magic file";
	my $line = 1;
	while (<MAGIC>) {
		$line++;

		if (/^\[(\d+):(.*?)\]\n$/) {
			push @magic, [$1,$2];
			next;
		}

		s/^(\d*)>(\d+)=(.{2})//s
			|| warn "$file line $line skipped\n" && next;
		my ($i, $o, $l) = ($1, $2, unpack 'n', $3);
		                  # indent, offset, value length
		while (length($_) <= $l) {
			$_ .= <MAGIC>;
			$line++;
		}

		my $v = substr $_, 0, $l, ''; # value

		/^(?:&(.{$l}))?(?:~(\d+))?(?:\+(\d+))?\n$/s
			|| warn "$file line $line skipped\n" && next;
		my ($m, $w, $r) = ($1, $2 || 1, $3 || 1);
		                  # mask, word size, range
		my $mdef = defined $m;

		# possible big endian to little endian conversion
		# as a bonus perl also takes care of weird endian cases
		if ( $w != 1 ) {
			my ( $utpl, $ptpl );
			if ( 2 == $w ) {
				$v = pack 'S', unpack 'n', $v;
				$m = pack 'S', unpack 'n', $m if $mdef;
			}
			elsif ( 4 == $w ) {
				$v = pack 'L', unpack 'N', $v;
				$m = pack 'L', unpack 'N', $m if $mdef;
			}
			else {
				warn "Unsupported word size: $w octets ".
				     " at $file line $line\n"
			}
		}

		my $end = $o + $l + $r - 1;
		$max_buffer = $end if $max_buffer < $end;
		my $ref = $i ? _find_branch($i) : $magic[-1];
		$r--;             # 1-based => 0-based range for regex
		$r *= 8 if $mdef; # bytes => bits for matching a mask
		my $reg = '^'
			. ( $r    ? "(.{0,$r}?)" : '()'           )
			. ( $mdef ? '('. _mask_regex($v, $m) .')'
			          : '('. quotemeta($v)       .')' ) ;
		push @$ref, [
			$o, $end,    # offset, offset+length+range
			$mdef,       # boolean for mask
			qr/$reg/sm,  # the regex to match
			undef	     # debug data
		];
		$$ref[-1][-1] = "$file line $line" if $DEBUG;
	}
	close MAGIC;
}

sub _find_branch { # finds last branch of tree of rules
	my $i = shift;
	my $ref = $magic[-1];
	for (1..$i) { $ref = $$ref[-1] }
	return $ref;
}

sub _mask_regex { # build regex based on mask
	my ($v, $m) = @_;
	my @v = split '', unpack "b*", $v;
	my @m = split '', unpack "b*", $m;
	my $re = '';
	for (0 .. $#m) {
		$re .= $m[$_] ? $v[$_] : '.' ;
		# If $mask = 1 than ($input && $mask) will be same as $input
		# If $mask = 0 than ($input && $mask) is always 0
		# But $mask = 0 only makes sense if $value = 0
		# So if $mask = 0 we ignore that bit of $input
	}
	return $re;
}

sub _escape_bytes { # used for debug output
	my $string = shift;
	if ($string =~ /[\x00-\x1F\x7F]/) {
		$string = join '', map {
			my $o = ord($_);
			($o < 32)   ? '^' . chr($o + 64) :
			($o == 127) ? '^?'               : $_ ;
		} split '', $string;
	}
	return $string;
}

1;

__END__

=head1 NAME

File::MimeInfo::Magic - Determine file type with magic

=head1 SYNOPSIS

	use File::MimeInfo::Magic;
	my $mime_type = mimetype($file);

=head1 DESCRIPTION

This module inherits from L<File::MimeInfo>, it is transparent
to its functions but adds support for the freedesktop magic file.

Magic data is hashed when you need it for the first time.
If you want to force hashing earlier use the C<rehash()> function.

=head1 EXPORT

The method C<mimetype> is exported by default. The methods C<magic>,
C<inodetype>, C<globs> and C<describe> can be exported on demand.

=head1 METHODS

See also L<File::MimeInfo> for methods that are inherited.

=over 4

=item C<mimetype($file)>

Returns a mime-type string for C<$file>, returns undef on failure.

This method bundles C<inodetype()>, C<globs()> and C<magic()>.

Magic rules with an priority of 80 and higher are checked before
C<globs()> is called, all other magic rules afterwards.

If this doesn't work the file is read and the mime-type defaults
to 'text/plain' or to 'application/octet-stream' when the first ten chars
of the file match ascii control chars (white spaces excluded).
If the file doesn't exist or isn't readable C<undef> is returned.

If C<$file> is an object reference only C<magic> and the default method
are used. See below for details.

=item C<magic($file)>

Returns a mime-type string for C<$file> based on the magic rules,
returns undef on failure.

C<$file> can be an object reference, in that case it is supposed to have a
C<seek()> and a C<read()> method. This allows you for example to determine
the mimetype of data in memory by using L<IO::Scalar>.

Be aware that when using a filehandle or an C<IO::> object you need to set
the C<:utf8> binmode yourself if apropriate.

=item C<rehash()>

Rehash the data files. Glob and magic
information is preparsed when this method is called.

If you want to by-pass the XDG basedir system you can specify your database
directories by setting C<@File::MimeInfo::DIRS>. But normally it is better to
change the XDG basedir environment variables.

=item C<default>

=item C<describe>

=item C<extensions>

=item C<globs>

=item C<inodetype>

These routines are imported from L<File::MimeInfo>.

=back

=head1 SEE ALSO

L<File::MimeInfo>

=head1 LIMITATIONS

Only word sizes of 1, 2 or 4 are supported. Any other word size is ignored
and will cause a warning.

=head1 BUGS

Please mail the author when you encounter any bugs.

=head1 AUTHOR

Jaap Karssenberg E<lt>pardus@cpan.orgE<gt>

Copyright (c) 2003, 2012 Jaap G Karssenberg. All rights reserved.
This program is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.
