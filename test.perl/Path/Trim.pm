package Path::Trim;

use warnings;
use strict;

use version; our $VERSION = qv('0.0.3');

sub new {
    return bless {}, __PACKAGE__;
}

sub get_current_directory {
    my $self = shift;
    return $self->{'current_directory'} || '.';
}

sub set_current_directory {
    my ( $self, $current_directory ) = @_;
    return $self->{'current_directory'} = $current_directory;
}

sub get_parent_directory {
    my $self = shift;
    return $self->{'parent_directory'} || '..';
}

sub set_parent_directory {
    my ( $self, $parent_directory ) = @_;
    return $self->{'parent_directory'} = $parent_directory;
}

sub get_directory_separator {
    my $self = shift;
    return $self->{'directory_separator'};
}

sub set_directory_separator {
    my ( $self, $directory_separator ) = @_;
    return $self->{'directory_separator'} = $directory_separator;
}

sub trim_path {
    my ( $self, $path ) = @_;
    my $current_directory   = $self->get_current_directory();
    my $parent_directory    = $self->get_parent_directory();
    my $directory_separator = $self->get_directory_separator();
    my ( $left_stripped, $right_stripped );
    if ( $path =~ s/^\Q$directory_separator\E// ) {
        $left_stripped = 1;
    }
    if ( $path =~ s/\Q$directory_separator\E$// ) {
        $right_stripped = 1;
    }
    my @dirs = split /\Q$directory_separator\E/, $path;
    my @new_path;

    for my $dir (@dirs) {
        if ( $dir eq $parent_directory ) {
            if ( @new_path && $new_path[-1] ne $parent_directory ) {
                pop @new_path;
            }
            else {
                push @new_path, $parent_directory;
            }
        }
        elsif ( $dir ne $current_directory ) {
            push @new_path, $dir;
        }
    }

    if ( !@new_path ) {
        push @new_path, $current_directory;
    }

    my $new_path = join $directory_separator, @new_path;
    if ($left_stripped) {
        $new_path = $directory_separator . $new_path;
    }
    if ($right_stripped) {
        $new_path = $new_path . $directory_separator;
    }
    return $new_path;
}

1;
__END__

=head1 NAME

Path::Trim - Makes paths compact

=head1 VERSION

This document describes Path::Trim version 0.0.3


=head1 SYNOPSIS

    use Path::Trim;

    my $path = 'a/b/../.././c/d';
    my $pt = Path::Trim->new();
    $pt->set_directory_separator('/');
    $trimmed_path = $pt->trim_path($path);


=head1 DESCRIPTION

Trims paths containing redundant current directory and parent directory entries.

For example:

    a/b/../.././c/d

is equivalent to:

    c/d


=head1 INTERFACE 

=over 4

=item C<< new() >>

Returns an object of Path::Trim.

=item C<< get_current_directory() >>

Returns the current directory representation.

Example:

    .

Defaults to C<.> if not set with C<set_current_directory()>.

=item C<< set_current_directory( current_directory ) >>

Sets the current directory representation to the value of C<current_directory>.

=item C<< get_parent_directory() >>

Returns the parent directory representation.

Example:

    ..

Defaults to C<..> if not set with C<set_parent_directory()>.

=item C<< set_parent_directory( parent_directory ) >>

Sets the parent directory representation to the value of C<parent_directory>.

=item C<< get_directory_separator() >>

Returns the directory separator, if it has already been set with
C<set_directory_separator()>.

=item C<< set_directory_separator( directory_separator ) >>

Sets the directory separator to the value of C<directory_separator>.

=item C<< trim_path( path ) >>

Accepts a path (without volume) and returns the trimmed version. It assumes that
a directory separator has already been set with C<set_directory_separator()>.

B<This is different from C<Cwd>'s C<abs_path()> in that it does not have any
filesystem ties.>

=back


=head1 CONFIGURATION AND ENVIRONMENT

Path::Trim requires no configuration files or environment variables.


=head1 DEPENDENCIES

None.


=head1 INCOMPATIBILITIES

None reported.


=head1 BUGS AND LIMITATIONS

No bugs have been reported.

Please report any bugs or feature requests to
C<bug-path-trim@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org>.


=head1 AUTHOR

Alan Haggai Alavi  C<< <haggai@cpan.org> >>


=head1 LICENCE AND COPYRIGHT

Copyright (c) 2010, 2011 Alan Haggai Alavi C<< <haggai@cpan.org> >>. All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.


=head1 DISCLAIMER OF WARRANTY

BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE SOFTWARE IS WITH
YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL
NECESSARY SERVICING, REPAIR, OR CORRECTION.

IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE
LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL,
OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE
THE SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF
SUCH DAMAGES.
