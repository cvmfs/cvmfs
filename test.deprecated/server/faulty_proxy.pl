#!/usr/bin/perl
# Originally taken from http://stackoverflow.com/questions/273718
# CreativeCommons ShareAlike: http://creativecommons.org/licenses/by-sa/2.5/

use strict;
use warnings;
use Data::Dumper;
use HTTP::Daemon;
use HTTP::Status;
use LWP::UserAgent;
use IO::Handle;

STDOUT->autoflush(1);
STDERR->autoflush(1);

my $port = shift;
my @faultyurls = @ARGV;
my $force_backend = "";
if (($faultyurls[0] eq "none") and defined($faultyurls[1])) {
  $force_backend = $faultyurls[1];
}

my $ua = LWP::UserAgent->new;
my $d=new HTTP::Daemon(LocalPort=>$port);
print "Please contact me at: < ", $d->url, " >\n";
if ($force_backend eq "") {
  foreach my $url (@faultyurls) {
    print "Will fail at $url\n";  
  }
} else {
  print "Forcing backend $force_backend\n";
}
while (my $c = $d->accept) {
  while (my $r = $c->get_request) {
    print "requested " . $r->url->path . "\n";
    my $faulty = 0; 
    foreach my $url (@faultyurls) {
      $faulty = 1 if (($r->url->path eq $url) or ($url eq "all"));
    }
    
    if ($faulty and (!defined($r->headers->{'cache-control'}) or ($r->headers->{'cache-control'} ne "no-cache"))) {
    #if (($r->url->path eq $faultyurl)) {
      print "Delivering crap\n";   
      $c->send_file_response("/tmp/cvmfs.faulty");
      print Dumper($r);
    } else {
      my $uri = ($force_backend eq "") ? $r->uri : ($force_backend . $r->url->path);
      print "asking backend server using uri $uri\n";
      my $response = $ua -> request( HTTP::Request->new(
        $r->method, 
        $uri, 
        $r->headers, 
        $r->content));
      print Dumper($response->{'_headers'});
      $c->send_response($response);
    }
    print "answer delivered\n";
  }
}

