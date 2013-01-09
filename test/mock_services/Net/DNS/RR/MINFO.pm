package Net::DNS::RR::MINFO;
#
# $Id: MINFO.pm 932 2011-10-26 12:40:48Z willem $
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
		($self->{"rmailbx"}, $offset) = Net::DNS::Packet::dn_expand($data, $offset);
		($self->{"emailbx"}, $offset) = Net::DNS::Packet::dn_expand($data, $offset);
	}

	return bless $self, $class;
}

sub new_from_string {
	my ($class, $self, $string) = @_;

	if ($string && ($string =~ /^(\S+)\s+(\S+)$/)) {
		$self->{"rmailbx"} = Net::DNS::stripdot($1);
		$self->{"emailbx"} = Net::DNS::stripdot($2);
	}

	return bless $self, $class;
}

sub rdatastr {
	my $self = shift;

	return $self->{"rmailbx"}
	       ? "$self->{rmailbx}. $self->{emailbx}."
	       : '';
}

sub rr_rdata {
	my ($self, $packet, $offset) = @_;
	my $rdata = "";

	if (exists $self->{"rmailbx"}) {
		$rdata .= $packet->dn_comp($self->{"rmailbx"}, $offset);

		$rdata .= $packet->dn_comp($self->{"emailbx"},
					   $offset + length $rdata);
	}

	return $rdata;
}


sub _normalize_dnames {
	my $self=shift;
	$self->_normalize_ownername();
	$self->{'rmailbx'}=Net::DNS::stripdot($self->{'rmailbx'}) if defined $self->{'rmailbx'};
	$self->{'emailbx'}=Net::DNS::stripdot($self->{'emailbx'}) if defined $self->{'emailbx'};
}



sub _canonicalRdata {
	my ($self, $packet, $offset) = @_;
	my $rdata = "";

	if (exists $self->{"rmailbx"}) {
		$rdata .= $self->_name2wire(lc($self->{"rmailbx"}));
		$rdata .=  $self->_name2wire(lc($self->{"emailbx"}));
	}

	return $rdata;
}


1;
__END__

=head1 NAME

Net::DNS::RR::MINFO - DNS MINFO resource record

=head1 SYNOPSIS

C<use Net::DNS::RR>;

=head1 DESCRIPTION

Class for DNS Mailbox Information (MINFO) resource records.

=head1 METHODS

=head2 rmailbx

    print "rmailbx = ", $rr->rmailbx, "\n";

Returns the RR's responsible mailbox field.  See RFC 1035.

=head2 emailbx

    print "emailbx = ", $rr->emailbx, "\n";

Returns the RR's error mailbox field.

=head1 COPYRIGHT

Copyright (c) 1997-2002 Michael Fuhr.

Portions Copyright (c) 2002-2004 Chris Reinhardt.

All rights reserved.  This program is free software; you may redistribute
it and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<perl(1)>, L<Net::DNS>, L<Net::DNS::Resolver>, L<Net::DNS::Packet>,
L<Net::DNS::Header>, L<Net::DNS::Question>, L<Net::DNS::RR>,
RFC 1035 Section 3.3.7

=cut
