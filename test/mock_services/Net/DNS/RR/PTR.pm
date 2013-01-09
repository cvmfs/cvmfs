package Net::DNS::RR::PTR;
#
# $Id: PTR.pm 966 2011-12-06 21:05:59Z willem $
#
use strict;
BEGIN {
    eval { require bytes; }
}
use vars qw(@ISA $VERSION);

@ISA     = qw(Net::DNS::RR);
$VERSION = (qw$LastChangedRevision: 966 $)[1];

sub new {
	my ($class, $self, $data, $offset) = @_;

	if ($self->{"rdlength"} > 0) {
		($self->{"ptrdname"}) = Net::DNS::Packet::dn_expand($data, $offset);
	}

	return bless $self, $class;
}

sub new_from_string {
	my ($class, $self, $string) = @_;

	if ($string) {
		$self->{"ptrdname"} = Net::DNS::stripdot($string);
	}

	return bless $self, $class;
}

sub rdatastr {
	my $self = shift;
	return "" unless defined $self->{ptrdname};
	return "$self->{ptrdname}." ;
}

sub rr_rdata {
	my ($self, $packet, $offset) = @_;
	my $rdata = "";

	if (exists $self->{"ptrdname"}) {
		$rdata .= $packet->dn_comp($self->{"ptrdname"}, $offset);
	}

	return $rdata;
}



sub _normalize_dnames {
	my $self=shift;
	$self->_normalize_ownername();
	$self->{'ptrdname'}=Net::DNS::stripdot($self->{'ptrdname'}) if defined $self->{'ptrdname'};
}


sub  _canonicalRdata {
	my ($self, $packet, $offset) = @_;
	my $rdata = "";

	if (exists $self->{"ptrdname"}) {
		$rdata .= $self->_name2wire(lc($self->{"ptrdname"}));
	}

	return $rdata;
}

1;
__END__

=head1 NAME

Net::DNS::RR::PTR - DNS PTR resource record

=head1 SYNOPSIS

C<use Net::DNS::RR>;

=head1 DESCRIPTION

Class for DNS Pointer (PTR) resource records.

=head1 METHODS

=head2 ptrdname

    print "ptrdname = ", $rr->ptrdname, "\n";

Returns the domain name associated with this record.

=head1 COPYRIGHT

Copyright (c) 1997-2002 Michael Fuhr.

Portions Copyright (c) 2002-2004 Chris Reinhardt.

All rights reserved.  This program is free software; you may redistribute
it and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<perl(1)>, L<Net::DNS>, L<Net::DNS::Resolver>, L<Net::DNS::Packet>,
L<Net::DNS::Header>, L<Net::DNS::Question>, L<Net::DNS::RR>,
RFC 1035 Section 3.3.12

=cut
