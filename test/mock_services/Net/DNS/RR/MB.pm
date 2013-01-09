package Net::DNS::RR::MB;
#
# $Id: MB.pm 932 2011-10-26 12:40:48Z willem $
#
use strict;
BEGIN {
    eval { require bytes; }
}
use vars qw(@ISA $VERSION);

@ISA     = qw(Net::DNS::RR);
$VERSION = (qw$LastChangedRevision: 932 $)[1];

sub new {
	my ($class, $self, $data, $offset) = @_;

	if ($self->{"rdlength"} > 0) {
		($self->{"madname"}) = Net::DNS::Packet::dn_expand($data, $offset);
	}

	return bless $self, $class;
}

sub new_from_string {
	my ($class, $self, $string) = @_;

	if ($string) {
		$self->{"madname"} = Net::DNS::stripdot($string);
	}

	return bless $self, $class;
}

sub rdatastr {
	my $self = shift;

	return $self->{"madname"} ? "$self->{madname}." : '';
}

sub rr_rdata {
	my ($self, $packet, $offset) = @_;
	my $rdata = "";

	if (exists $self->{"madname"}) {
		$rdata .= $packet->dn_comp($self->{"madname"}, $offset);
	}

	return $rdata;
}


sub _normalize_dnames {
	my $self=shift;
	$self->_normalize_ownername();
	$self->{"madname"}=Net::DNS::stripdot($self->{"madname"}) if defined $self->{"madname"};
}



sub _canonicalRdata {
    my $self=shift;
    my $rdata = "";
    if (exists $self->{"madname"}) {
		$rdata .= $self->_name2wire(lc($self->{"madname"}));
	}
	return $rdata;
}


1;
__END__

=head1 NAME

Net::DNS::RR::MB - DNS MB resource record

=head1 SYNOPSIS

C<use Net::DNS::RR>;

=head1 DESCRIPTION

Class for DNS Mailbox (MB) resource records.

=head1 METHODS

=head2 madname

    print "madname = ", $rr->madname, "\n";

Returns the domain name of the host which has the specified mailbox.

=head1 COPYRIGHT

Copyright (c) 1997-2002 Michael Fuhr.

Portions Copyright (c) 2002-2004 Chris Reinhardt.

All rights reserved.  This program is free software; you may redistribute
it and/or modify it under the same terms as Perl itself.
=head1 SEE ALSO

L<perl(1)>, L<Net::DNS>, L<Net::DNS::Resolver>, L<Net::DNS::Packet>,
L<Net::DNS::Header>, L<Net::DNS::Question>, L<Net::DNS::RR>,
RFC 1035 Section 3.3.3

=cut
