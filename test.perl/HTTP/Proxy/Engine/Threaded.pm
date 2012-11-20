package HTTP::Proxy::Engine::Threaded;
use strict;
use HTTP::Proxy;
use threads;

# A massive hack of Engine::Fork to use the threads stuff
# Basically created to work under win32 so that the filters
# can share global caches among themselves
# Angelos Karageorgiou angelos@unix.gr

our @ISA = qw( HTTP::Proxy::Engine );
our %defaults = (
    max_clients => 60,
);


__PACKAGE__->make_accessors( qw( kids select ), keys %defaults );

sub start {
    my $self = shift;
    $self->kids( [] );
    $self->select( IO::Select->new( $self->proxy->daemon ) );
}

sub run {
    my $self   = shift;
    my $proxy  = $self->proxy;
    my $kids   = $self->kids;

    # check for new connections
    my @ready = $self->select->can_read(1);
    for my $fh (@ready) {    # there's only one, anyway
        # single-process proxy (useful for debugging)

        # accept the new connection
        my $conn  = $fh->accept;
	my $child=threads->new(\&worker,$proxy,$conn);
        if ( !defined $child ) {
            $conn->close;
            $proxy->log( HTTP::Proxy::ERROR, "PROCESS", "Cannot spawn thread" );
            next;
        }
	$child->detach();

    }

}

sub stop {
    my $self = shift;
    my $kids = $self->kids;

   # not needed
}

sub worker {
	my $proxy=shift;
	my $conn=shift;
       $proxy->serve_connections($conn);
	$conn->close();
       return;
}

1;

__END__

=head1 NAME

HTTP::Proxy::Engine::Threaded - A scoreboard-based HTTP::Proxy engine

=head1 SYNOPSIS

    my $proxy = HTTP::Proxy->new( engine => 'Threaded' );

=head1 DESCRIPTION

This module provides a threaded engine to HTTP::Proxy.

=head1 METHODS

The module defines the following methods, used by HTTP::Proxy main loop:

=over 4

=item start()

Initialize the engine.

=item run()

Implements the forking logic: a new process is forked for each new
incoming TCP connection.

=item stop()

Reap remaining child processes.

=back

=head1 SEE ALSO

L<HTTP::Proxy>, L<HTTP::Proxy::Engine>.

=head1 AUTHOR

Angelos Karageorgiou C<< <angelos@unix.gr> >>. (Actual code)

Philippe "BooK" Bruhat, C<< <book@cpan.org> >>. (Documentation)

=head1 COPYRIGHT

Copyright 2010, Philippe Bruhat.

=head1 LICENSE

This module is free software; you can redistribute it or modify it under
the same terms as Perl itself.

=cut

