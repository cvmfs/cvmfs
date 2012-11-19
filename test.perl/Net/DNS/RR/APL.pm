package Net::DNS::RR::APL;
#
# $Id: APL.pm 932 2011-10-26 12:40:48Z willem $
#
use strict;
BEGIN {
    eval { require bytes; }
}
use vars qw(@ISA $VERSION);

@ISA     = qw(Net::DNS::RR);
$VERSION = (qw$LastChangedRevision: 684 $)[1];

use Net::DNS::RR::APL::ApItem;

1;

=head1 NAME

Net::DNS::RR::APL - DNS APL resource record

=head1 SYNOPSIS

C<use Net::DNS::RR>;

=head1 DESCRIPTION

This is an RR type for address prefix lists. Please see the RFC3123 for details.


=head1 METHODS

=head2 applist

Returns an array of Net::DNS::APL::ApItem objects.

Each ApItem objecst contains the following attribute that can be
accessed and set using methods of the same name: addressfamily,
prefix, negation, address

  foreach my $ap ($apl->aplist()){
     	print $ap->negation()?"!":"";
    	print $ap->addressfamily().":";
    	print $ap->address();
    	print $ap->prefix(). " ";
    }

In addition the  Net::DNS::APL::ApItem objects can be printed using
the string method.

    foreach my $ap ($apl->aplist())
              print $ap->string."\n";
    }




=cut





sub new {
	my ($class, $self, $data, $offset) = @_;
	my $max=$offset+$self->{"rdlength"};

	while ($offset < $max){
		my $apitem;
		($apitem,$offset)=Net::DNS::RR::APL::ApItem->new_from_wire($$data,$offset);
		push @{$self->{"aplist"}}, $apitem if defined ($apitem);
	}
	bless $self, $class;
}



sub new_from_string {
	my ($class, $self, $string) = @_;
	my @input=split(/\s+/, $string);
	foreach my $i (@input){
		my $apitem=Net::DNS::RR::APL::ApItem->new($i);
		push @{$self->{"aplist"}}, $apitem if defined ($apitem);
	}
	return bless $self, $class;
}




sub rdatastr {
	my $self = shift;
	my $rdatastr="";
	foreach my $apitem (@{$self->{'aplist'}}){
		$rdatastr.=$apitem->string." ";
	}
	chop $rdatastr; #trailing space
	return $rdatastr;
}

sub rr_rdata {
	my $self = shift;
	my $rdata = "";

	foreach my $apitem (@{$self->{'aplist'}}){
		$rdata.=$apitem->rdata;
	}
	return $rdata;
}



sub aplist {
	my $self=shift;
	return @{$self->{'aplist'}};
}




=head1 COPYRIGHT

Copyright (c) 2008 Olaf Kolkman (NLnet Labs)

All rights reserved.  This program is free software; you may redistribute
it and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<Net::DNS::RR::APL::ApItem>,
L<perl(1)>, L<Net::DNS>, L<Net::DNS::Resolver>, L<Net::DNS::Packet>,
L<Net::DNS::Header>, L<Net::DNS::Question>, L<Net::DNS::RR>,
RFC 3123


=cut

