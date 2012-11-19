package Net::DNS::RR::APL::ApItem;
#
# $Id: ApItem.pm 932 2011-10-26 12:40:48Z willem $
#


use Net::DNS::Resolver::Base;
use Socket;
use strict;
BEGIN {
    eval { require bytes; }
}
use vars qw(@ISA $VERSION);

@ISA     = qw(Net::DNS::RR);

$VERSION = (qw$LastChangedRevision: 684 $)[1];


use Data::Dumper;



=head1 NAME

Net::DNS::RR::APL::ApItem - DNS APL  ApItem


=head1 SYNOPSIS

use Net::DNS::RR::APL::ApItem;

my $apitem=Net::DNS::RR::APL::ApItem->new();

=head1 DESCRIPTION

This is an object that contains the Address Prefixes with related
information such as address family and possible negation flags for the
APL RR. Please see the RFC3123 for details.


=head1 Methods

=head2 new

Constructor method.

$apitem[0]=Net::DNS::RR::APL::ApItem->new(1:192.168.32.0/21);
$apitem[1]=Net::DNS::RR::APL::ApItem->new(!1:192.168.32.0/21);
$apitem[2]=Net::DNS::RR::APL::ApItem->new(2:FF00:0:0:0:0:0:0:0/8);

$empty=Net::DNS::RR::APL::ApItem();

Returns undef if the argument cannot be parsed.
Returns an object with empty attributes when no argument is supplied.


=cut

sub new {
	my $caller = shift;
	my $class = ref($caller) || $caller;
	my $self={
		  address => '',
		  addressfamily => '',
		  negation => '',
		  prefix => '',
		 };
	my $input=shift;
	bless $self, $class;
	return $self->parse($input) if defined($input);  # returns undef when parse fails
	return $self;
}



#  The textual representation of an APL RR in a DNS zone file is as
#   follows:
#
#   <owner>   IN   <TTL>   APL   {[!]afi:address/prefix}*
#
#   The data consists of zero or more strings of the address family
#   indicator <afi>, immediately followed by a colon ":", an address,
#   immediately followed by the "/" character, immediately followed by a
#   decimal numeric value for the prefix length.  Any such string may be
#   preceded by a "!" character.  The strings are separated by
#   whitespace.  The <afi> is the decimal numeric value of that
#   particular address family.

sub parse {
	my $self=shift;
	my $input=shift;
	if($input=~/(!?)([12])\:(.+)\/(\d{1,3})/){
		my $negation=$1;
		my $addressfamily=$2;
		my $address=$3;
		my $prefix=$4;
		# Sanity check
		return if ( ($addressfamily == 1) && (! Net::DNS::Resolver::Base::_ip_is_ipv4($address) || $prefix > 32 ));
		return if ( ($addressfamily == 2) && (! Net::DNS::Resolver::Base::_ip_is_ipv6($address) || $prefix > 128 ));
		return if ( $addressfamily > 2 );

		$self->address($address);
		$self->addressfamily($addressfamily);
		$self->prefix($prefix);
		$self->negation($negation);

		return $self;
	}
	#Pattern match failed.
	return;
}
sub print {	print &string, "\n"; }



=head2 string

Returns a string representation of the object in the  '[!]afi:address/prefix' format
   $apl=$apitem[0]->string;


=head2 print

Prints the textual representation of the object.
   $apitem[0]->print;
   # same functionality as:
   $apl=$apitem[0]->string;
   print $apl;


=cut

sub string {
	my $self=shift;
	my $dat= $self->negation ? "!" : "";
	$dat .= $self->addressfamily() .":" .
	  $self->address."/".$self->prefix;
	return $dat;
}




# APL RDATA format
#
#   The RDATA section consists of zero or more items (<apitem>) of the
#   form
#
#      +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
#      |                          ADDRESSFAMILY                        |
#      +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
#      |             PREFIX            | N |         AFDLENGTH         |
#      +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
#      /                            AFDPART                            /
#      |                                                               |
#      +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
#
#      ADDRESSFAMILY     16 bit unsigned value as assigned by IANA
#                        (see IANA Considerations)
#      PREFIX            8 bit unsigned binary coded prefix length.
#                        Upper and lower bounds and interpretation of
#                        this value are address family specific.
#      N                 negation flag, indicates the presence of the
#                        "!" character in the textual format.  It has
#                        the value "1" if the "!" was given, "0" else.
#      AFDLENGTH         length in octets of the following address
#                        family dependent part (7 bit unsigned).
#      AFDPART           address family dependent part.  See below.
#
#  @self

sub rdata {
	my $self=shift;
	my $rdata;
	return unless ((exists $self->{'addressfamily'}) &&
		       (exists $self->{'address'}) &&
		       (exists $self->{'prefix'}));

	if ($self->{'addressfamily'}==1){
		$rdata=pack ("n",0x0001);
		$rdata.=pack ("C",$self->prefix());
		my $data .= &stripzerobytes(inet_aton($self->address()));
		$rdata.=pack("C", ($self->negation()? (0x80|length($data)):length($data) ));
		$rdata.=$data;
	}elsif ($self->{'addressfamily'}==2){
		$rdata=pack ("n",0x0002);
		$rdata.=pack ("C",$self->prefix());
		my @addr = split(/:/, $self->{"address"});
		my $data=&stripzerobytes(pack("n8", map { hex $_ } @addr ));
		$rdata.=pack("C", ($self->negation()? (0x80|length($data)):length($data) ));
	        $rdata.=$data;
	}else{
		#NOT Defined, return empty A data
		return;
	}

	return $rdata;
}


sub stripzerobytes {
	my @a = unpack('C*',shift);
	my @b;
	my $notseenval=1;
	while (defined(my $a=pop( @a))){
		unshift @b,$a unless ($a==0 && $notseenval);
		next unless $notseenval;
		$notseenval= !$a;
	}
	return pack("C*",@b);
}

sub new_from_wire {
	my $caller = shift;
	my $class = ref($caller) || $caller;
	my ($data, $offset) = @_;

	my $orgoffset=$offset;
	my $self={
		  address => '',
		  addressfamily => '',
		  negation => '',
		  prefix => '',
		 };
	bless $self, $class;

	$self->addressfamily( unpack('n',substr($data,$offset,2)));
	$offset+=2;
	$self->prefix( unpack('C',substr($data,$offset,1)));
	$offset+=1;
	my $negandlength=unpack('C',substr($data,$offset,1));
	$self->negation( 0x80 == ( $negandlength & 0x80));
	$offset+=1;
	my $length= $negandlength & 0x7f;
	return if (length($data)< $offset+$length);
	if ($self->addressfamily==1){
		# pack(Cy,unpack("C*",$foo)) is a hack to pad with trailing 0s.
		$self->{"address"} =
		  inet_ntoa(  pack("C4",unpack("C*",substr($data, $offset,$length) )));
	}elsif($self->addressfamily==2){
		my $extracted=substr($data, $offset,$length);
		# pack(Cy,unpack("C*",$foo)) is a hack to pad with trailing 0s.
		my @addr = unpack("n8", pack("C16",unpack("C*",substr($data, $offset,$length) )));
		$self->{"address"} = sprintf("%x:%x:%x:%x:%x:%x:%x:%x", @addr);
	}else{
		#FORMERR
		return;
	}
	$offset+=$length;
	return $self,$offset;
}






=head1 COPYRIGHT

Copyright (c) 2008 Olaf Kolkman (NLnet Labs)

All rights reserved.  This program is free software; you may redistribute
it and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<Net::DNS::RR::APL>,
L<perl(1)>, L<Net::DNS>, L<Net::DNS::Resolver>, L<Net::DNS::Packet>,
L<Net::DNS::Header>, L<Net::DNS::Question>, L<Net::DNS::RR>,
RFC 3123


=cut

