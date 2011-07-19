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
      $q->h1("Usage: store.pl?[key=/key_id/&content=/content/&vo=/VO/ ($more_info)"),
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
(my $keyid = $q->param('key')) =~ /[a-f0-9]{40}/ || usage('wrong key id');
(my $content = $q->param('content')) =~ /[a-f0-9]{64}/ || usage('wrong key');
(my $vo = $q->param('vo')) =~ /[a-zA-Z0-9_-]{1,40}/ || usage('wrong vo');

# check sha1 against packed content
my $sha1 = sha1_hex($content);
if ($sha1 ne $keyid) {
   usage('key id is not SHA1 hash of key')
}

# check for key (required for the list), store key->value
my $exists = $r->exists("cvmfskey:$keyid");
$r->set("cvmfskey:$keyid" => "$content;$vo") || einternal("Failed to store catalog key");

# store in cvmfskeys list
if (!$exists) {
   $r->lpush("cvmfskeys" => "$keyid") || einternal("Failed to register key");
}


print $q->header,
   $q->start_html('OK'),
   $q->h1('OK (' . $ENV{'SSL_CLIENT_S_DN_OU'} . ')');
print $q->end_html;

