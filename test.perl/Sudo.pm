package Sudo;

use IPC::Run qw(run timeout start harness);
use Term::ReadPassword;
use base qw(Class::Accessor);

use strict;
our $VERSION = '0.32';


sub sudo_run
    {
     # Ok, glue the bits together.  
     my $self = shift;
     my (%ret,$binary,$sudo,$program,$command,$sudo_args,$program_args);
     my ($final_cmd,$sudo_pipe_handle,@cmd,$in,$out,$err,$line);
     my ($username,$remote_machine,$remote_user);

     # <deep sigh>  Ok, IPC::Run is very (I mean very) sensitive to 
     # its @cmd processing.  Each option has to be its own entry
     # in the array.  This means that options that flow in, if they
     # are not of the form -\w but are -\w\s+\S+ (e.g. an option with
     # an option parameter, such as -u username), have to be broken up
     # into their own array elements in @cmd. 
     #
     # Hopefully IPC::Run will be fixed one day.  Until then, we write 
     # to the real API (not the documented one).
     # </deep sigh>

     # do we have a binary ...
     if ($self->{debug})
        {
	  $ENV{IPCRUNDEBUG}='basic';
	  if ($self->{debug} == 2) 
	     {
	       $ENV{IPCRUNDEBUG}='data';	       
	     } 
	  elsif ($self->{debug} >= 3) 
	     {
	       $ENV{IPCRUNDEBUG}='details';	       
	     } 
	}
     if (!defined($self->{sudo}))
        {
	  %ret = {
	          'error' => 'Error: you did not tell me where the sudo binary is was not set'
	         };
	  return \%ret;
	}
	
     $self->{sudo} =~ /^(\S+)$/;    # force binary to be a
     				    # single string with no
				    # spaces.  This may break
				    # some folks paths, but it
				    # should be safer than allowing
				    # any command string.
     $sudo = $1;
     
     # test for remote execution ... you need to have the ssh keys
     # setup before this ...     
     #$remote_machine=$self->{hostname}if (defined($self->{hostname}));
     if (defined($remote_machine))
        {
	  $remote_user	= getpwuid($<);  # default user name is the user running the script
	  if (defined($self->{username}))
	     {
	       $remote_user	= $self->{username};
	     }
	   push @cmd,"ssh";
	   push @cmd, (sprintf '%s@%s',$remote_user,$remote_machine);
	}
     if (!defined($remote_machine))
        {   		    
	 if (! -e $sudo )
            {
	      %ret = {
	              'error' => (sprintf 'Error: the sudo binary "%s" does not exist',$sudo)
	             };
	      return \%ret;
	    }

	 if (! -x $sudo )
            {
	      %ret = {
	              'error' => (sprintf 'Error: the sudo binary "%s" is not executable',$sudo)
	             };
	      return \%ret;
	    }
	}
     push @cmd,$sudo;

     # force the -S switch to take the password from 
     # STDIN
     push @cmd,'-S';
     
     if (!exists($self->{_timeout})) { $self->{_timeout} = 10; }
     
     $program_args	= "";     
     
     # ok, append the user information
     if (!defined($self->{username}))
        {
	  %ret = {
	          'error' => 'Error:  username was not set'
	         };
	  return \%ret;
	}
     
     if (exists	($self->{sudo_args}))
        {
	  # process the arguments, splitting on white space
	  $self->{sudo_args} =~ s/^\s+//;  # trim leading spaces
	  $self->{sudo_args} =~ s/\s+$//;  # trim trailing spaces
	  push @cmd,(split(/\s+/,$self->{sudo_args})); 	
	}
	
     push @cmd,"-u";
     push @cmd,$self->{username};

     if (!defined($self->{program}))
        {
	  %ret = {
	          'error' => 'Error: the program attribute was not set'
	         };
	  return \%ret;
	}
     $self->{program} =~ /^(\S+)$/;  # force binary to be a
     				    # single string with no
				    # spaces.  This may break
				    # some folks paths, but it
				    # should be safer than allowing
				    # any command string.
     $program = $1;			    
     if (!defined($remote_machine) && (! -e $program ))
        {
	  %ret = {
	          'error' => (sprintf 'Error: the program "%s" does not exist',$program)
	         };
	  return \%ret;
	}

     if (!defined($remote_machine) && (! -x $program ))
        {
	  %ret = {
	          'error' => (sprintf 'Error: the program "%s" is not executable',$program)
	         };
	  return \%ret;
	}
     push @cmd,$program;

     if (exists	($self->{program_args}))
        {
	  # process the arguments, splitting on white space
	  $self->{program_args} =~ s/^\s+//;  # trim leading spaces
	  $self->{program_args} =~ s/\s+$//;  # trim trailing spaces
	  # Note:  this might break some programs due to the
	  # multiple ways options may be specified and the actual
	  # command line argument contents.  Hopefully it will be
	  # ok to start...	 
	  push @cmd,(split(/\s+/,$self->{program_args})); 	
	}
 
 
      
     # ok, build the final "options" to the sudo we are going to run
     $command = join(" ",@cmd);
     printf STDERR "- command: %s\n",$command if ($self->{debug});     
     if (
	 exists($self->{debug}) &&
	 $self->{debug} >= 2     
        )
        {
	 my $index=0;
	 foreach my $entry (@cmd)
	   {
             printf STDERR "- _ [%s]: %s\n",$index,$entry;
	     $index++;
	   }
       }
     printf STDERR "\n< username = %s\n",$username if ( 
     					        exists($self->{debug}) &&
						$self->{debug} >= 2
					      );
     printf STDERR "< password = %s\n",$self->{password} if ( 
     					        exists($self->{debug}) &&
						$self->{debug} >= 2
					      );
     printf STDERR "< sudo = %s\n",$sudo if ( 
     					        exists($self->{debug}) &&
						$self->{debug} >= 2
					      );
     printf STDERR "< program = %s\n",$program if ( 
     					        exists($self->{debug}) &&
						$self->{debug} >= 2
					      );
     printf STDERR "< program args = %s\n",$program_args if ( 
     					        exists($self->{debug}) &&
						$self->{debug} >= 2
					      );
     
     
     printf STDERR ": starting sudo \n" if ($self->{debug}); 
     
     $in=$self->{password};
     $out="";
     $err="";
     my $h = run \@cmd,\$in,\$out,\$err,timeout($self->{_timeout});
     
     
     printf STDERR "\n\n> output: %s \n> result: %s\n\n",$out,$? if ($self->{debug}); 
     
     %ret = (
              'stdout'	=>	$out,
	      'stderr'	=>	$err,
	      'rc'	=>	$h
            );
	    
     return \%ret;
  }   

sub sudo_shell_start
    {
     # Ok, glue the bits together.  
     my $self = shift;
     my (%ret,$binary,$sudo,$program,$command,$sudo_args,$program_args);
     my ($final_cmd,$sudo_pipe_handle,@cmd,$in,$out,$err,$line);
     my ($username,$remote_machine,$remote_user);

     # <deep sigh>  Ok, IPC::Run is very (I mean very) sensitive to 
     # its @cmd processing.  Each option has to be its own entry
     # in the array.  This means that options that flow in, if they
     # are not of the form -\w but are -\w\s+\S+ (e.g. an option with
     # an option parameter, such as -u username), have to be broken up
     # into their own array elements in @cmd. 
     #
     # Hopefully IPC::Run will be fixed one day.  Until then, we write 
     # to the real API (not the documented one).
     # </deep sigh>

     # do we have a binary ...
     if ($self->{debug})
        {
	  $ENV{IPCRUNDEBUG}='basic';
	  if ($self->{debug} == 2) 
	     {
	       $ENV{IPCRUNDEBUG}='data';	       
	     } 
	  elsif ($self->{debug} >= 3) 
	     {
	       $ENV{IPCRUNDEBUG}='details';	       
	     } 
	}
     if (!defined($self->{sudo}))
        {
	  %ret = {
	          'error' => 'Error: you did not tell me where the sudo binary is was not set'
	         };
	  return \%ret;
	}
	
     $self->{sudo} =~ /^(\S+)$/;    # force binary to be a
     				    # single string with no
				    # spaces.  This may break
				    # some folks paths, but it
				    # should be safer than allowing
				    # any command string.
     $sudo = $1;
     
     if (! -e $sudo )
        {
	  %ret = {
	          'error' => (sprintf 'Error: the sudo binary "%s" does not exist',$sudo)
	         };
	  return \%ret;
	}

     if (! -x $sudo )
        {
	  %ret = {
	          'error' => (sprintf 'Error: the sudo binary "%s" is not executable',$sudo)
	         };
	  return \%ret;
	}
     push @cmd,$sudo;

     # force the -S switch to take the password from 
     # STDIN
     push @cmd,'-S';

     # force the -s switch to use a shell
     push @cmd,'-S';
     
     
     # ok, append the user information
     if (!defined($self->{username}))
        {
	  %ret = {
	          'error' => 'Error:  username was not set'
	         };
	  return \%ret;
	}
     
     if (exists	($self->{sudo_args}))
        {
	  # process the arguments, splitting on white space
	  $self->{sudo_args} =~ s/^\s+//;  # trim leading spaces
	  $self->{sudo_args} =~ s/\s+$//;  # trim trailing spaces
	  push @cmd,(split(/\s+/,$self->{sudo_args})); 	
	}
	
     push @cmd,"-u";
     push @cmd,$self->{username};

      
     # ok, build the final "options" to the sudo we are going to run
     $command = join(" ",@cmd);
     printf STDERR "- command: %s\n",$command if ($self->{debug});     
     if (
	 exists($self->{debug}) &&
	 $self->{debug} >= 2     
        )
        {
	 my $index=0;
	 foreach my $entry (@cmd)
	   {
             printf STDERR "- _ [%s]: %s\n",$index,$entry;
	     $index++;
	   }
       }
     printf STDERR "\n< username = %s\n",$username if ( 
     					        exists($self->{debug}) &&
						$self->{debug} >= 2
					      );
     printf STDERR "< password = %s\n",$self->{password} if ( 
     					        exists($self->{debug}) &&
						$self->{debug} >= 2
					      );
     printf STDERR "< sudo = %s\n",$sudo if ( 
     					        exists($self->{debug}) &&
						$self->{debug} >= 2
					      );     
     
     printf STDERR ": starting sudo \n" if ($self->{debug}); 
     
     $self->{in}=$self->{password};
     $self->{out}="";
     $self->{err}="";
     $self->{sudo_handle} = start \@cmd,\$self->{in},\$self->{out},\$self->{err};
     
     
     printf STDERR "\n\n> output: %s \n> result: %s\n\n",$out,$? if ($self->{debug}); 
     
  }   

sub sudo_shell_pump_nb
    {
      my ($self)=shift;
      $self->{handle}->pump_nb;
    }
1;
__END__

=head1 NAME

Sudo - Perl extension for running a command line sudo

=head1 SYNOPSIS

  use Sudo;
  my $su;
  
  $su = Sudo->new(
  		  {
		   sudo 	=> '/usr/bin/sudo',
		   sudo_args	=> '...',		   		   
		   username	=> $name, 
		   password	=> $pass,
		   program	=> '/path/to/binary',
		   program_args	=> '...',
		  # and for remote execution ...

		  [hostname	=> 'remote_hostname',]
		  [username	=> 'remote_username']

		  }
		 );
   
  $result = $su->sudo_run();
  if (exists($result->{error})) 
     { 
       &handle_error($result); 
     }
    else
     {
       printf "STDOUT: %s\n",$result->{stdout};
       printf "STDERR: %s\n",$result->{stderr};
       printf "return: %s\n",$result->{rc};
     }
  

=head1 DESCRIPTION

Sudo runs commands as another user, provided the system sudo 
implementation is setup to enable this.  This does not allow
running applications securely, simply it allows the programmer to
run a program as another user (suid) using the sudo tools rather
than suidperl.  Suidperl is not generally recommended for 
secure operation as another user.  While sudo itself is a single 
point tool to enable one user to execute commands as another
sudo does not itself make you any more or less secure.

Warning:  This module does not make your code any more or less
secure, it simply uses a different mechanism for running as a
different user.  This requires a properly configured sudo system to
function.  It has all the limitations/restrictions of sudo.  It has
an added vulnerability, in that you may need to provide a
plain-text password in a variable.  This may be attackable.  Future
versions of the module might try to address this.

This module specifically runs a single command to get output which 
is passed back to the user.  The module does not currently allow for 
interactive bidirectional communication between the callee and caller.
The module does not spool input into the callee.

=head1 Methods

=over 4

=item new

The C<new> function creates a new object instance and sets the 
attributes of the object as presented in a hash form.  Relevant 
attributes are formed as key => value pairs in a hash as follows:

 
=over 6

=item username => $username

Name of the user that will be used to run the command.  Equivalent to
the C<-u username> option to sudo.  Note:  You may use C<#uid> rather
than the username.  

=item sudo => $sudo_binary

Fully qualified path to sudo.  No whitespace allowed.  This should be
a single continuous string with path separators, and should include
the sudo binary file name.  E.g. under various Linux, this attribute might
be set as C<sudo => '/usr/bin/sudo'>.

=item sudo_args => $sudo_args

Any additional sudo arguments you would like to use for the program you
are trying to run.  

=item program => $program

Fully qualified path to program binary you wish to run.  No whitespace
allowed.  This should be a single continuous string with path 
separators, and should include the program binary file name.  

=item program_args => $program_args

Arguments to pass to program you wish to run.

=back

=back

=over 4
 
=item sudo_run

The C<sudo_run> function first checks the attributes to make sure
the minimum required set exists, and then attempts to execute 
sudo without shell interpolation.  You will need to take this into
account in case you get confusing failure modes.  You may set the
debug attribute to 1, 2, or 3 to get progressively more information.

The object will return a hash.  The hash will have state
information  within it. If the C'error' key exists, an error
occured and you can  parse the value to see what the error
was.  If the run was successful, C'stdout' key exists, and its
value corresponds to stdout output from the program run by
sudo.  Similarly the C'stderr' key will exist for a successful
run, and the value corresponds to stderr output from the program
run by sudo.  The C"rc" key will also be defined with the
programs return code.

=back

=head1 TODO

B<I/O> currently this is a fancy way to run a command as another
user without being suid.  Eventually it may evolve to 
have IO:: goodness, or similar functionality.

=head1 BUGS

As this module depends upon IPC::Run, it has all the bugs/limitations 
of IPC::Run.  Spaces in file names, executables, and other I<odd> items 
which give IPC::Run fits, will give Sudo fits.  We would like to fix this, 
but this requires fixing IPC::Run.

Insecurity as a bug.  Security is not a product or feature.  It is a process. 
If your systems are grossly insecure to begin with, using Sudo will not
help you.  Good security practice (not draconian security practice)
is recommended across all systems.  Basic common sense on services, 
file ownership, remote access, and so forth go a long way to helping 
the process.  Start with the basics.  Work from there.  


=head1 SEE ALSO

sudo(8), perl(1), IPC::Run, a good book on locking down a system



=head1 AUTHOR

Joe Landman, B<landman@scalableinformatics.com>, L<http://www.scalableinformatics.com>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2004,2005 by Scalable Informatics LLC

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8 or,
at your option, any later version of Perl 5 you may have available.
=cut
