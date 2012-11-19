package Net::DNS::RR::HIP;
#
# $Id: HIP.pm 932 2011-10-26 12:40:48Z willem $
#
# See RFC 5205 for the specification of this record.

use strict;
use MIME::Base64;
use Data::Dumper;

use vars qw(@ISA $VERSION);

@ISA     = qw(Net::DNS::RR);
$VERSION = (qw$LastChangedRevision: 718 $)[1];






sub new {
	my ($class, $self, $data, $offset) = @_;

        if ($self->{'rdlength'} > 0) {
                my $offset_pkalgorithm  = $offset+1;
                my $offset_pklength     = $offset+2;
                my $offset_hit          = $offset+4;

                $self->{'_hitlength'} = unpack('C', substr($$data, $offset, 1));
		my $offset_pubkey    = $offset_hit + $self->{'_hitlength'};

                $self->{'pkalgorithm'}    = unpack('C', substr($$data, $offset_pkalgorithm, 1));
                $self->{'_pklength'}    = unpack('n', substr($$data, $offset_pklength, 2));
		my $offset_rendezvous    = $offset_pubkey + $self->{'_pklength'};

                $self->{'hitbin'}    = substr($$data, $offset_hit, $self->{'_hitlength'});
		$self->{'hit'}=unpack("H*",$self->{'hitbin'});

		$self->{'pubkeybin'}= substr($$data,$offset_pubkey, $self->{'_pklength'});
		$self->{'pubkey'}=encode_base64($self->{'pubkeybin'},"");
                my $rsoffset    = $offset_pubkey + $self->{'_pklength'};
		$self->{'rendezvousservers'}= [];



		my $i=10;
		while( ($rsoffset-$offset)<$self->{'rdlength'}){
			exit unless $i--;
			my ($name, $nextoffset) = Net::DNS::Packet::dn_expand($data, $rsoffset);
			push (@{$self->{'rendezvousservers'}},$name);
			$rsoffset=$nextoffset;

		}
	}

	return bless $self, $class;
}



sub new_from_string {
	my ($class, $self, $string) = @_;
	# first turn multiline into single line
	$string =~ tr/()//d if $string;
	$string =~ s/\n//mg if $string;

	if ($string && ($string =~ /^\s*(\d+)\s+(\S+)\s+(.*)$/)) {
		@{$self}{qw(pkalgorithm hit)} = ($1, $2);
		$self->{'hitbin'}=pack("H*",$self->{'hit'});
		my $reststring=$3;
		# rest string are the space separated components of the base64 encoded public key
		# appended by fully qualified domain names.
		# We'll chop off the FQDNs
		$self->{'rendezvousservers'}=[];
		while ($reststring =~ s/^(.*)(\s+((\S+\.)(\S+\.?)*))\s*$/$1/s){
			unshift (@{$self->{'rendezvousservers'}},$3);
		}
		$reststring=~s/\s//g;
		return () if (length($reststring) % 4); #base64 length should be mulitple of 4
		$self->{'pubkey'}=$reststring;
		$self->{'pubkeybin'} =  decode_base64( $self->{'pubkey'} );
      	}

	return bless $self, $class;
}




sub rr_rdata {
	my ($self, $packet, $offset) = @_;
	my $rdata = "";

	if (exists $self->{"pubkey"}) {
		# This is for consistency.
		my $hitbin=$self->hitbin();
		my $pubkeybin=$self->pubkeybin();
		$rdata = pack("C", $self->{'_hitlength'});
		$rdata .= pack("C", $self->{'pkalgorithm'});
		$rdata .= pack("n", $self->{'_pklength'});
		$rdata .= $hitbin;
		$rdata .= $pubkeybin;
		foreach my $dname (@{$self->{'rendezvousservers'}}){
			$rdata .= $self->_name2wire ($dname);
		}
	}

	return $rdata;
}



sub rdatastr {
	my $self = shift;
	my $rdatastr='';

	if (exists $self->{"pubkey"}) {
		$rdatastr = $self->pkalgorithm       . ' '   .
		            $self->hit  . ' '  .
		            $self->pubkey       . ' ';

		foreach my $dname ( @{$self->rendezvousservers()} ) {
			$rdatastr .= $dname.". ";
		}
		chop $rdatastr;

	}

	return $rdatastr;
}


sub hitbin {
	my ($self, $new_val) = @_;
	if (defined $new_val) {
		$self->{'hitbin'} = $new_val;
		$self->{'hit'}=unpack("H*",$new_val);
	}
	$self->{'hitbin'}=pack("H*",$self->{'hit'}) unless defined ($self->{'hitbin'});
	$self->{'_hitlength'} =length($self->{'hitbin'});
	return ($self->{'hitbin'});
}


sub hit {
	my ($self, $new_val) = @_;
	if (defined $new_val) {
		$self->{'hitbin'} = $new_val;
		$self->{'hitbin'}=pack("H*",$new_val);
	}
	$self->{'hit'}=unpack("H*",$self->{'hitbin'}) unless defined ($self->{'hit'});
	$self->{'_hitlength'} =length($self->{'hitbin'});
	return ($self->{'hit'});
}




sub pubkeybin {
	my ($self, $new_val) = @_;
	if (defined $new_val) {
		$self->{'pubkeybin'} = $new_val;
		$self->{'pubkey'}=encode_base64($self->{'pubkeybin'},"");

	}
	$self->{'pubkeybin'}= decode_base64($self->{'pubkey'}) unless defined ($self->{'pubkeybin'});
	$self->{'_pklength'} =length($self->{'pubkeybin'});
	return ($self->{'pubkeybin'});
}






sub pubkey {
	my ($self, $new_val) = @_;
	if (defined $new_val) {
		$self->{'pubkey'} = $new_val;
		$self->{'pubkeybin'}=decode_base64($self->{'pubkey'});

	}
	$self->{'pubkey'}= encode_base64($self->{'pubkeybin'},"") unless defined ($self->{'pubkey'});
	$self->{'_pklength'} =length($self->{'pubkeybin'});
	return ($self->{'pubkey'});
}





sub _normalize_dnames {
	my $self=shift;
	$self->_normalize_ownername();
	$self->{'rendezvousservers'} ||= [];
	my @dnames = @{$self->{'rendezvousservers'}};
	$self->{'rendezvousservers'}=[];
	foreach my $dname (@dnames){
		push (   @{$self->{'rendezvousservers'}},   Net::DNS::stripdot($dname) )
	}


}


sub rendezvousservers {
	my ($self, $new_val) = @_;

	if ($new_val) {
		$self->{'rendezvousservers'}= $new_val;
	}

	$self->_normalize_dnames();
	return $self->{'rendezvousservers'};

}










=head1 NAME

Net::DNS::RR::HIP - DNS HIP resource record

=head1 SYNOPSIS

C<use Net::DNS::RR>;

=head1 DESCRIPTION

This class implements the HIP RR (RFC5205)


=head1 METHODS

=head2 pkalgorithm

Returns or sets the public key algorithm field

=head2 hit

Returns or sets the hit in base16 representation.

=head2 hitbin

Returns or sets the binary representation of the the hit.

Using hit or hitbin to set the one of these attributes will update both attributes.

=head2 pubkey

Returns or sets the publick key in base64 representation.

=head2 pubkey

Returns or sets the binary representation of the the public key.

Using pubkey or pubkeybin to set the one of these attributes will update both attributes.



=head2 rendezvousservers


      my $rendezvousservers=$hip->rendezvousservers();

Returns a reference to an array of rendezvous servers. The representation is in
Perl's internal storage format i.e. without trailing dot.

     $hip->rendezvousservers( [ qw|example.com  example.net| ] )

With a reference to an array as the argument this method will set the rendezvousservers.



=head1 NOTES

Since (multiline) base64 encoded publik keys may contain spaces string
parsing of the HIP RR depends on rendevous server names containing at
least one . (dot) in their domain name. Failure of string parsing will
return an 'undef'.

The rdatastr method (and hence the string and print methods) return the
rendezvousservers as fully qualified domain names.



=head1 COPYRIGHT

Copyright (c) 2009 Olaf Kolkman (NLnet Labs)

All rights reserved.  This program is free software; you may redistribute
it and/or modify it under the same terms as Perl itself.

=head1 SEE ALSO

L<perl(1)>, L<Net::DNS>, L<Net::DNS::Resolver>, L<Net::DNS::Packet>,
L<Net::DNS::Header>, L<Net::DNS::Question>, L<Net::DNS::RR>,
RFC 5205


=cut




1;
