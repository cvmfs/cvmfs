package Functions::Setup;

####################################################
# Here will be stored all the functions needed to setup the environment
# for the daemon
####################################################

use strict;
use warnings;
use Fcntl ':mode';

# The next line is here to help me find the directory of the script
# if you have a better method, let me know.
use FindBin qw($RealBin);

# Next lines are needed to export subroutines to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(setup fixperm);

sub check_daemon {
	my $running = `ps -ef | grep cvmfs-testdwrapper | grep -v grep | grep -v defunct`;
	return $running;
}

sub setup {
	my $fixperm = shift;
	
	if (!check_daemon()){
		my $user_id = `id -u`;
		chomp($user_id);
		if ($user_id ne '0') {
			print "You will need to be in the sudoers file to complete the setup process. Are you? [N,y]";
			my $answer = <STDIN>;
			unless ($answer eq "y\n" or $answer eq "Y\n") { return }
		}			
			my $compile_success = compile_wrapper();
			my $user_added = create_user();
			my $wrapper_chown = chown_wrapper();
			my $wrapper_setuid = setuid_wrapper();
			my $log_folder = create_log_folder();
			my $add_to_sudoers = add_to_sudoers();
		
		unless(defined($fixperm) and $fixperm eq 'true') {
			print "\n";
			print '_' x 80 . "\n";
			print "Setup complete. Run 'fixperm' to be sure all permission are set correctly.\n";
			print "You can type 'help fixperm' to show what this command does.\n";
			print '_' x 80 . "\n";
		}
		else {
			fixperm();
		}			
	}
	else {
		print "The daemon is running. Can't run setup while the daemon is running.\nStop it and retry.\n";
	}
}

# This function will try to compile the c wrapper for the script. It will return 1 on success and 0 on failure.
sub compile_wrapper {
	if (-e '/usr/bin/gcc') {
		print "Compiling the wrapper... \n";
		my $compile = system("sudo /usr/bin/gcc -o $RealBin/cvmfs-testdwrapper $RealBin/cvmfs-testdwrapper.c");
		if ($compile == -1){
			print "FAILED: $!\n";
			return 0;
		}
		else {
			print ("Done.\n");
			return 1;
		}
	}
	else {
		print "It seems you don't have gcc installed on your system. Install it and retry.\n";
		return 0;
	}
}

# This function will try to create a new user for the daemon, return 1 on success and 0 on failure
sub create_user {
	# Checking if the user exists in the system
	my $user = `cat /etc/passwd | grep cvmfs-test`;
	# If it doesn't, create it
	if (!$user) {
		if (-e '/usr/sbin/useradd') {
			print "Adding user 'cvmfs-test'.\n";
			my $added = system('sudo /usr/sbin/useradd -r --key UMASK=0000 cvmfs-test');
			if ($added == -1) {
				print "FAILED: $!\n";
				return 0;
			}
			else {
				print "User 'cvmfs-test' added.\n";
				return 1;
			}
		}
		else {
			print "It seems you don't have useradd on your system. Install it and retry.\n";
			return 0;
		}
	}
	else {
		print "User already present on the system.\n";
		return 1;
	}
}

# This function will check if the wrapper is owned by the user cvmfs-test, return 1 on success and 0 on failure
sub chown_wrapper {
	# Checking if the user own the daemon wrapper
	my $uid = (stat("$RealBin/cvmfs-testdwrapper"))[4];
	my $owner = (getpwuid($uid))[0];
	if($owner ne 'root') {
		print "Changing the owner of the wrapper... \n";
		my $chowned = system("sudo chown root:root $RealBin/cvmfs-testdwrapper");
		if ($chowned == -1){
			print "FAILED: $!\n";
			return 0;
		}
		else {
			print "Done.\n";
			return 1;
		}
	}
	else {
		print "Wrapper already owned by cvmfs-test.\n";
		return 1;
	}
}

# This function will add the setuid byte to wrapper permission
sub setuid_wrapper {
	# Checking if the file has the setuid bit
	my $mode = (stat("$RealBin/cvmfs-testdwrapper"))[2];
	my $suid = $mode & S_ISUID;
	
	if($suid) {
		print "setuid byte already set on the wrapper.\n";
		return 1;
	}
	else {
		print "Adding setuid byte... \n";
		my $setuid = system("sudo chmod u+s $RealBin/cvmfs-testdwrapper");
		if ($setuid == -1){
			print "FAILED: $!\n";
			return 0;
		}
		else {
			print "Done.\n";
			return 1;
		}
	}
}

# This function will set all permission for files and directories
sub fixperm {
	my $user_id = `id -u`;
	chomp($user_id);
	if ($user_id ne '0') {
		print "You will need to be in the sudoers file to complete the setup process. Are you? [N,y]";
		my $answer = <STDIN>;
		unless ($answer eq "y\n" or $answer eq "Y\n") { return }
	}
	
	print 'Setting permission for modules to 644... ';
	system("sudo find $RealBin -type f -name \"*.pm\" -exec chmod 644 {} +");
	print "Done.\n";
	
	print 'Setting permission for executables to 755... ';
	system("sudo find $RealBin -type f -name \"*.pl\" -exec chmod 755 {} +");
	print "Done.\n";
	
	print 'Setting permission for directories to 755... ';
	system("sudo find $RealBin -type d -exec chmod 755 {} +");
	print "Done.\n";
	
	print 'Setting permission for tests directory to 777... ';
	system("sudo find $RealBin -name \"Tests*\" -type d -exec chmod 777 {} +");
	print "Done.\n";
	
	print 'Setting permission for help files to 644... ';
	system("sudo find $RealBin -type f -name \"*help\" -exec chmod 644 {} +");
	print "Done.\n";
	
	print 'Setting permission for bash script to 755... ';
	system("sudo find $RealBin -type f -name \"*.sh\" -exec chmod 755 {} +");
	print "Done.\n";

	print 'Setting permission for archives to 777... ';
	system("sudo find $RealBin -type f -name \"*.tar.gz\" -exec chmod 777 {} +");
	print "Done.\n";

	print 'Setting permission for C executable files to 755... ';
	system("sudo find $RealBin -type f -name \"*.crun\" -exec chmod 755 {} +");
	print "Done.\n";
	
	print 'Setting permission for pod files to 644...';
	system("sudo find $RealBin -type f -name \"*.pod\" -exec chmod 644 {} +");
	print "Done.\n";
}

sub create_log_folder {
	unless( -e '/var/log/cvmfs-test' ) {
		print 'Creating /var/log/cvmfs-test... ';
		system("sudo mkdir -p /var/log/cvmfs-test");
		print "Done.\n";
	}
	else {
		print "Log folder already present on the system.\n";
	}
	
	my $uid = (stat('/var/log/cvmfs-test'))[4];
	my $owner = (getpwuid($uid))[0];
	
	unless ($uid eq 'cvmfs-test') {
		print 'Setting owner and permission on the log folder...';
		system("sudo chown cvmfs-test /var/log/cvmfs-test");
		system("sudo chmod 777 /var/log/cvmfs-test");
		print "Done.\n";
	}
	else {
		print "Log folder already owned by user cvmfs-test.\n";
	}
}

sub add_to_sudoers {
	my $sudoers = `sudo cat /etc/sudoers | grep "cvmfs-test ALL=(ALL) NOPASSWD: ALL"`;
	if ($sudoers) {
		print "User cvmfs-test is already in the /etc/sudoers file.\n";
	}
	else {
		print 'Adding cvmfs-test to the /etc/sudoers file...';
		system ('sudo sh -c "echo \"cvmfs-test ALL=(ALL) NOPASSWD: ALL\" >> /etc/sudoers"');
		print "Done.\n";
	}
}

1;
