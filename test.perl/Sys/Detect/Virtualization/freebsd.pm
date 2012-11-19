package Sys::Detect::Virtualization::freebsd;
use warnings;
use strict;

use base qw( Sys::Detect::Virtualization );

=head1 NAME

Sys::Detect::Virtualization::freebsd - Detection of virtualization under a FreeBSD system

=head1 DESCRIPTION

See L<Sys::Detect::Virtualization> for usage information.

=head1 METHODS

=head2 Internal Methods

=over 4

=item new ( )

Constructor.  You should not invoke this directly.  Instead, use L<Sys::Detect::Virtualization>.

=cut

sub new
{
	my ($class) = @_;
	my $self = {};
	bless $self, $class;
	return $self;
}

=item detect_ps ( )

'ps' output on FreeBSD will show 'J' for jailed processes.

=cut

sub detect_ps
{
	my ($self) = @_;

	return $self->_check_command_output(
		'ps -o stat',
		[
			# FreeBSD jail
			qr/J/       => [ $self->VIRT_FREEBSD_JAIL ],
		],
	);
}

=item detect_dmesg ( )

Check the output of the 'dmesg' command for telltales.

=cut

sub detect_dmesg
{
	my ($self) = @_;

	return $self->_check_command_output(
		$self->_find_bin('dmesg'),
		[
			# Qemu / KVM
			qr/qemu harddisk/i => [ $self->VIRT_KVM, $self->VIRT_QEMU ],
			qr/qemu dvd-rom/i  => [ $self->VIRT_KVM, $self->VIRT_QEMU ],
		],
	);

}

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2009 Roaring Penguin Software Inc.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;
