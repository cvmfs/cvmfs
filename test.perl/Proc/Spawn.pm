
require 5.6.0;
package Proc::Spawn;
use strict;
use POSIX;
use IO;

## Module Version
our $VERSION = 1.03;

require Exporter;
our @ISA    = qw(Exporter);
our @EXPORT = qw(spawn);

# Spawn using pipes
#
# This should be used for programs which do not open /dev/tty, to
# avoid wasting ptys.
#
#  ($pid, $in_fh, $out_fh, $err_fh) = spawn(ARGS);
#
#  Where ARGS are one of:
#   "command and arguments"
#   ["command","and","arguments"]
#
sub spawn ($) {
  my ($cmd) = @_;

  # Create pipes to use for stdio
  my ( $inC, $inP ) = POSIX::pipe();
  die "Cannot create pipe: $!\n" unless defined $inC;
  my ($outP, $outC) = POSIX::pipe();
  die "Cannot create pipe: $!\n" unless defined $outP;
  my ($errP, $errC) = POSIX::pipe();
  die "Cannot create pipe: $!\n" unless defined $errP;

  # Create a child to exec the command
  my $pid = fork;
  die "Cannot fork: $!\n" unless defined $pid;

  unless ( $pid ) { # Child
    # Close shared stdio
    close STDIN;
    close STDOUT;
    close STDERR;

    # Open stdio on pipes
    open(STDIN, "<&$inC");
    open(STDOUT,">&$outC");
    open(STDERR,">&$errC");

    # Sanity check
    die "Stdio not opened properly\n" unless fileno(STDERR) == 2;

    # Close unneeded filehandles
    POSIX::close($inC);
    POSIX::close($outC);
    POSIX::close($errC);
    POSIX::close($inP);
    POSIX::close($outP);
    POSIX::close($errP);

    # Run the command
    if ( ref($cmd) =~ /ARRAY/ ) {
      exec @$cmd;
      die "Cannot exec @$cmd: $!\n";
    } else {
      exec $cmd;
      die "Cannot exec $cmd: $!\n";
    }
  }

  # Parent
  POSIX::close($inC);
  POSIX::close($outC);
  POSIX::close($errC);

  $inP  = new_from_fd IO::Handle($inP,  'w');
  $outP = new_from_fd IO::Handle($outP, 'r');
  $errP = new_from_fd IO::Handle($errP, 'r');

  $inP->autoflush(1);
  return ($pid, $inP, $outP, $errP);
}

1;

__END__

=head1 NAME

Proc::Spawn - Run external programs

=head1 SYNOPSIS

  use Proc::Spawn;

  my ($pid, $in_fh, $out_fh, $err_fh) = spawn("...");
  my ($pid, $in_fh, $out_fh, $err_fh) = spawn(["...", ...]);

  my ($pid, $pty_fh) = spawn_pty("...");
  my ($pid, $pty_fh) = spawn_pty(["...", ...]);

=head1 DESCRIPTION

B<Proc::Spawn> runs external programs, like B<ls> and B<telnet>.  The
process id of the spawned programs and B<IO::Handle> objects are
returned.

The B<spawn> function should be used for most purposes.  It returns
three B<IO::Handle> objects for stdin, stdout, and stderr of the
program being run.  This is sufficient for running nearly all
programs, and does not consume significant operating system resources.

The B<spawn_pty> function should only be used when running a program
that opens B</dev/tty> to communicate.  Examples of such programs are
B<telnet> and B<passwd>.  This function returns a single B<IO::Handle>
object that must be used for all input and output for the program.

=head1 ERRORS

The module will C<die> on errors.

=head1 NOTES

This module is UNIX oriented. Functionality on other systems may vary.

=head1 AUTHOR

John Redford, John.Redford@fmr.com

=head1 SEE ALSO

IO::Handle

=cut
