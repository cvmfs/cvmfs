#!/usr/bin/perl -T

use strict;
use warnings;
use CGI;
use Redis;
use Digest::SHA1 qw(sha1_hex);

my $q = CGI->new;

sub usage {
   my $more_info = shift;
   
   print $q->header(-type => 'text/html', -status => '400 Malformed Request'),
      $q->start_html('Usage'),
      $q->h1("Usage: entitlement.pl?[vo=/VO[:VO]"),
      $q->end_html;
      
   exit;
}

sub einternal {
   my $reason = shift;
   print $q->header(-type => 'text/html', -status => '503 Service Unavailable'),
      $q->start_html('Not Found'),
      $q->h1($reason),
      $q->end_html;
      
   exit;
}

# Store the symmetric key from ? parameters
my $r = Redis->new;
(my $vo = $q->param('vo')) =~ /[\w\-:]{1,40}/ || usage('wrong vo list');
my $entitlement = 'CVM-';

# Generate 16+32 Byte iv+key
my $key = '';
for (my $i = 0; $i < 32; $i++) {
   $key = $key . sprintf("%02x", int(rand(256)));
}
$entitlement = $entitlement . $key;

# Expiry date: 30 days
$entitlement = $entitlement . '-' . int(time + 3600*24*30);

# VOs
$entitlement = $entitlement . '-' . $vo;

# SHA1 hash as id
my $entitlement_id = sha1_hex($entitlement);

# check for key (required for the list), store key->value
my $exists = $r->exists("entitlement:$entitlement_id");
$r->set("entitlement:$entitlement_id" => "$entitlement") || einternal("Failed to store entitlement");

# store in cvmfskeys list
if (!$exists) {
   $r->lpush("entitlements" => "$entitlement_id") || einternal("Failed to register entitlement");
}

print $q->header(-type => 'text/plain'),
   $entitlement;
