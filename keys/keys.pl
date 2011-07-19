#!/usr/bin/perl

use strict;
use warnings;
use CGI;
use Redis;
use Digest::SHA1 qw(sha1_hex);
use Crypt::CBC;
use Crypt::OpenSSL::AES;

my $q = CGI->new;

sub usage() {
   print $q->header(-type => "text/html", -status => "400 Malformed Request"),
      $q->start_html('Usage'),
      $q->h1("Usage: keys.pl?key=/key_id/&entitlement=/entitlement_id/"),
      $q->end_html;
      
   exit;
}

sub enoent {
   my $missing = shift;
   print $q->header(-type => "text/html", -status => "404 Not Found"),
      $q->start_html('Not Found'),
      $q->h1("$missing Not Found"),
      $q->end_html;
   
   exit;
}

sub einternal {
   my $reason = shift;
   print $q->header(-type => "text/html", -status => "503 Service Unavailable"),
      $q->start_html('Service Unavailable'),
      $q->h1($reason),
      $q->end_html;
      
   exit;
}

sub eperm {
   my $reason = shift;
   print $q->header(-type => "text/html", -status => "403 Forbidden"),
      $q->start_html('Forbidden'),
      $q->h1($reason),
      $q->end_html;
   
   exit;
}


# Get the key and entitlement IDs from ? parameters
(my $key_id = $q->param('key')) =~ /[a-z0-9]{40}/ || usage();
(my $entitlement_id = $q->param('entitlement')) =~ /[a-z0-9]{40}/ || usage();

# Lookup key
my $r = Redis->new;
my $key_list = $r->get("cvmfskey:$key_id") || enoent("Key ID");
my $key; my $key_vo;
($key, $key_vo) = split(/;/, $key_list);
$key_id eq sha1_hex($key) || einternal("Bad Key");

# Lookup entitlement
my $entitlement_list = $r->get("entitlement:$entitlement_id") || enoent("Entitlement ID");
my $dummy; my $ent_key; my $ent_expiry; my $ent_vo_list;
($dummy, $ent_key, $ent_expiry, $ent_vo_list) = split(/-/, $entitlement_list);
$entitlement_id eq sha1_hex($entitlement_list) || einternal("Bad Entitlement");
defined($ent_expiry) || einternal("Bad Entitlement");
defined($ent_vo_list) || einternal("Bad Entitlement"); 

# Check expiry
if ($ent_expiry < time) {
   eperm('Entitlement expired');
}

# Check vos
my $vo_match=0;
for (split(/:/, $ent_vo_list)) {
   if ($_ eq $key_vo) {
      $vo_match = 1;
      last;
   }
}
if (!$vo_match) {
   eperm('Not entitled for key vo');
}

# Use the random iv and entitlement key as symmetric key to encrypt the catalog key
my $iv = '';
for (my $i = 0; $i < 16; $i++) {
   $iv = $iv . sprintf("%02x", int(rand(256)));
}
my $cipher_iv = pack('H*', $iv);
my $cipher_key = pack('H*', $ent_key);
my $cipher = Crypt::CBC->new(-key    => $cipher_key,
                             -iv     => $cipher_iv,
                             -literal_key => 1,
                             -cipher => "Crypt::OpenSSL::AES",
                             -header => 'none');
my $cipher_text = $cipher->encrypt($key) || einternal("Encryption error");

print $q->header(-type => "application/octet-stream",
                 -Content_length=>length($iv)+length($cipher_text)),
 $iv, $cipher_text;

