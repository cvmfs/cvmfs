package Functions::Help;

##############################
# Here will be stored all help related functions
##############################

use strict;
use warnings;
use File::Find;
use Functions::Shell qw(check_daemon);

# The next line is here to help me find the directory of the script
# if you have a better method, let me know.
use FindBin qw($RealBin);

# Next lines are needed to export subroutines to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(help);

# This functions will be launched everytime the user type the help command.
# The goal of this function is only to select wich other help functions is needed.
sub help {
	# Retrieving arguments
	my $line = shift;
	
	# Splitting $line in an array depending on blank...
	my @words = split /[[:blank:]]/, $line;
	# Everything else, if exist, are options.
	my @options = splice(@words, 1);
	
	# Changing operations to do depending on the daemon status
	if(Functions::Shell::check_daemon()) {
		if( @options and scalar(@options) > 1){
			print "Please, one command at time.\n";
		}
		elsif( @options and scalar(@options) == 1 and $options[0]){
			print_command_help($options[0]);
		}
		else{
			print_help();
		}
	}
	else {
		if( @options and scalar(@options) > 1){
			print "Please, one command at time.\n";
		}
		elsif( @options and scalar(@options) == 1 and $options[0]){
			print_shell_help($options[0]);
		}
		else {
			print_shell_help();
		}
	}
}

# This function will retrive the position of plugin related help file.
# It search recursively in all folder inside $RealBin (that is the directory
# where the script is located). In this way we can organize our plugins in
# "subject related" subfolder.
sub get_help_file {
	# Retrieving argument, the command asked
	my $command = shift;
	
	# Searching the right help file
	my $helpfile;
	my $select = sub {
		if ($File::Find::name =~ m/.*\/$command\/help/){	
			$helpfile = $&;		
		}
	};
	finddepth( { wanted => $select }, $RealBin);
	
	# Returning the help file path to print_command_help
	return $helpfile;
}

# This function will retrieve all the help file inside $RealBin and for each one of them
# it prints the Short description. It needs no argument.
sub print_help {	
	# Retrieving all help files
	my @helpfiles;
	my $select = sub {
		if($File::Find::name =~ m/.*\/help$/){			
			push @helpfiles,$File::Find::name; # adds help files in the @helpfiles array.
		}
	};
	find( { wanted => $select }, $RealBin);
	
	# Here it will open all the files and will print their contents. Only lines
	# starting with 'Short:'.
	foreach (@helpfiles) {
		open (my $file, $_);
		while (defined (my $line = $file->getline)){
			if($line =~ m/^Short:.*/){
				my @helpline = split /[:]/,$line,2;
				print $helpline[1];
			}
		}
		close $file;
	}
}

# This function will print the Long help of a specific help file, probably found thanks
# to the get_command_help() function. Probably it can be integrated in that function, as
# already done with generic help, I think they will be almost everytime used together.
sub print_command_help {
	# Retrieving argument: the command
	my $command = shift;
	
	# Checking if the command has a specific help file in shell_help
	if ($command eq 'setup' or $command eq 'fixperm' or $command eq 'start' or $command eq 'restart') {
		print_shell_help($command);
		return;
	}
	
	# Retrieving the right help file
	my $helpfile = get_help_file($command);
	
	# If the helpfile exists, now it's time to print it's content.
	if ( defined ($helpfile) && -e $helpfile){
		open (my $file, $helpfile);
		while (defined (my $line = $file->getline)){
			if($line =~ m/^$command:.*/){
				my @helpline = split /[:]/,$line,2;
				print $helpline[1];
			}
		}
		close $file;
	}
	else {
		print "No specific help file found for the command $command.\nType 'help' for a list of available commands.\n";
	}
}

# This function will retrieve the help for the shell when the daemon is not running.
sub print_shell_help {
	# Retrieving arguments: the command. The function can be used without arguments.
	my $command = shift;
	
	# Searching the right help file
	my $helpfile;
	my $select = sub {
		if ($File::Find::name =~ m/.*shell_help$/){	
			$helpfile = $&;		
		}
	};
	finddepth( { wanted => $select }, $RealBin);
	
	# Printing help information
	if ( defined ($helpfile) && -e $helpfile){
		open (my $file, $helpfile);
		while (defined (my $line = $file->getline)){
			if (!defined ($command)) {
				if($line =~ m/^Short:.*/){
					my @helpline = split /[:]/,$line,2;
					print $helpline[1];
				}
			}
			else  {
				if($line =~ m/^$command:.*/){
					my @helpline = split /[:]/,$line,2;
					print $helpline[1];
				}
			}
		}
		close $file;
	}
	else {
		print "No help file found for the shell.\n";
	}
}

1;
