package Net::DNS::RR::KX;
#
# $Id: MX.pm 718 2008-02-26 21:49:20Z olaf $
#


use Net::DNS::RR::MX;


@ISA     = qw(Net::DNS::RR::MX);
$VERSION = (qw$LastChangedRevision: 684 $)[1];

# Not inhereted since KX does not comress
sub rr_rdata {
	my ($self, $packet, $offset) = @_;
	my $rdata = "";

	if (exists $self->{"preference"}) {
		$rdata .= pack("n", $self->{"preference"});
		$rdata .= $self->_name2wire(lc($self->{"exchange"}));
	}

	return $rdata;
}




1;
__END__

=head1 NAME

Net::DNS::RR::KX - DNS KX resource record

=head1 SYNOPSIS

C<use Net::DNS::RR>;

=head1 DESCRIPTION

Class for DNS Key Exchanger (MX) resource record (RFC 2230).

This class inherets most of its functionality directly from the MX RR.

=head1 METHODS

=head2 preference

    print "preference = ", $rr->preference, "\n";

Returns the preference for this mail exchange.

=head2 exchange

    print "exchange = ", $rr->exchange, "\n";

Returns name of this mail exchange.

=head1 COPYRIGHT

Portions Copyright (c) 2009 Olaf Kolkman NLnet Labs.

All rights reserved.  This program is free software; you may redistribute
it and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<perl(1)>, L<Net::DNS>, L<Net::DNS::Resolver>, L<Net::DNS::Packet>,
L<Net::DNS::Header>, L<Net::DNS::Question>, L<Net::DNS::RR>,
RFC2230

=cut
