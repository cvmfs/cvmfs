package Net::DNS::RR::DHCID;
use Data::Dumper;


#
# $Id: DHCID.pm 932 2011-10-26 12:40:48Z willem $
#
use strict;
BEGIN {
    eval { require bytes; }
}
use vars qw(@ISA $VERSION );
use Socket;
use MIME::Base64;
use Digest::SHA  qw( sha256 );

$VERSION = (qw$LastChangedRevision: 718 $)[1];

@ISA = qw(Net::DNS::RR);




#   +------------------+------------------------------------------------+
#   |  Identifier Type | Identifier                                     |
#   |       Code       |                                                |
#   +------------------+------------------------------------------------+
#   |      0x0000      | The 1-octet 'htype' followed by 'hlen' octets  |
#   |                  | of 'chaddr' from a DHCPv4 client's DHCPREQUEST |
#   |                  | [7].                                           |
#   |      0x0001      | The data octets (i.e., the Type and            |
#   |                  | Client-Identifier fields) from a DHCPv4        |
#   |                  | client's Client Identifier option [10].        |
#   |      0x0002      | The client's DUID (i.e., the data octets of a  |
#   |                  | DHCPv6 client's Client Identifier option [11]  |
#   |                  | or the DUID field from a DHCPv4 client's       |
#   |                  | Client Identifier option [6]).                 |
#   |  0x0003 - 0xfffe | Undefined; available to be assigned by IANA.   |
#   |      0xffff      | Undefined; RESERVED.                           |
#   +------------------+------------------------------------------------+


sub new {
    my ($class, $self, $data, $offset) = @_;
    my $offsettoidentifiertype    = $offset;
    my $offsettodigesttype = $offset+2;
    my $offsettodigest    = $offset+3;

    bless $self, $class;

    $self->{'identifiertype'}=(unpack('n', substr($$data, $offsettoidentifiertype, 2)));
    $self->{'digesttype'}=(unpack('C', substr($$data, $offsettodigesttype, 1)));
    $self->{'digestbin'}=(substr($$data, $offsettodigest, $self->{'rdlength'}-3));
    $self->digest();
    $self->{'rdatastr'}=encode_base64 (substr($$data,$offset, $self->{'rdlength'}),"");
    return $self;

}



sub new_from_string {
	my ($class, $self, $string) = @_;
	# first turn multiline into single line
	$string =~ tr/()//d if $string;
	$string =~ s/\n//mg if $string;
	$string=~s/\s//g if $string;
	bless $self, $class;
	return $self unless $string;
	$self->{'rdatastr'}= $string;
	my $data=$self->rr_rdata();
	$self->{'identifiertype'} = unpack('n', substr($data, 0 , 2));
	$self->{'digesttype'}    = unpack('C', substr($data, 2, 1));
	$self->{'digestbin'}    =  substr($data, 3, length($data) - 3 );
	return $self;
}



sub rdatastr {
	my $self     = shift;

	return $self->{'rdatastr'};
}



# Overwrite the AUTOLOAD methods, we only want to read and not set.


sub digestbin {
	my ($self, $new_val) = @_;
	if (defined $new_val) {
		$self->{'digestbin'} = $new_val;
		$self->{'digest'}=encode_base64($self->{'digestbin'},"");

	}
	$self->{'digestbin'}= decode_base64($self->{'digest'}) unless defined $self->{'digestbin'};
	return ($self->{'digestbin'});
}


sub digest {
	my ($self, $new_val) = @_;
	if (defined $new_val) {
		$self->{'digest'} = $new_val;
		$self->{'digestbin'}=decode_base64($self->{'digest'});

	}
	$self->{'digest'}= encode_base64($self->{'digestbin'},"") unless defined ($self->{'digest'});
	return ($self->{'digest'});

}





sub rr_rdata {
	my $self=shift;
	my $rdata='';
	if ($self->{'digesttype'}) {
		$rdata = pack("n", $self->identifiertype);
		$rdata .= pack("C", $self->digesttype);
		$rdata .= $self->digestbin;
	}elsif(exists $self->{"rdatastr"}){
		$rdata .= decode_base64($self->{'rdatastr'});
	}
	return $rdata;

}



1;


=head1 NAME

Net::DNS::RR::IPSECKEY - DNS DHCID resource record

=head1 SYNOPSIS

C<use Net::DNS::RR>;

=head1 DESCRIPTION

CLASS for the DHCID RR.

=head1 METHODS

=head2 identifiertype

Returns the value of the identifiertype field.

=head2 identifiertype

Returns the value of the digesttype field.

=head2 digest

Returns the value of the digest in base64 encoding.

=head2 digestbin

Returns the value of the digest.

=head1 TODO/NOTES

While the various accessor methods can be used to read and set values the setting
of values is discouraged as those are not properly tested.

There should be a creator method that takes the identifier and fqnds as input and generates
all fields with some knowledge of RFC4703.

=head1 COPYRIGHT

Copyright (c) 2009 NLnet LAbs, Olaf Kolkman.

"All rights reserved, This program is free software; you may redistribute it
and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<perl(1)>, L<Net::DNS>, L<Net::DNS::Resolver>, L<Net::DNS::Packet>,
L<Net::DNS::Header>, L<Net::DNS::Question>, L<Net::DNS::RR>,
RFC4701

=cut





