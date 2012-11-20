# The main.pl file under the plugin folder will be automatically launched
# everytime you launch the command. Here you should write your script.

use strict;
use warnings;

###########
# MODULES #
###########

# A very good idea, at first, is to include the script directory in Perl include dir.
# Moreover, whenever you need to refer to the script directory, you will be able to do
# it with $RealBin.
use FindBin qw($RealBin);
use lib $RealBin;

# First of all, we have to include every module needed for the test.
# The most important module is, probably, the Tests::Common module, which
# contains a lot of function that will help you trough the creation
# of your test.
# We'll explain what this functions do when we'll use it in our test.
# We're including here just the fundamental function, you should have a look
# to Tests/Common.pm for a complete reference to offered function.
use Tests::Common qw(setup_environment get_daemon_output set_stdout_stderr
	                 open_test_socket close_test_socket open_shellout_socket
	                 check_repo);

# If you want your test to be able to launch other services (or test as well)
# you need to be able to speak with the server. So you want probably include
# the ZeroMQ module for socket managing. This is the modules used by the two
# function included above open_test_socket() and close_test_socket().
use ZeroMQ qw/:all/;


############################
# TEST NAME  AND REPO NAME #
############################

# Set a variable for the test name. It's required by the function open_test_socket(),
# and you'll probably find other situation where it's useful. It must be all
# capitalized.
my $testname = 'EDIT_REPO_BASIC';

# Some other variables that you could find useful to access the repo.
my $repo_name = '127.0.0.1';
my $tmp_repo = '/tmp/server/repo/';
my $repo_pub = $tmp_repo . 'pub';

########################
# COMMAND LINE OPTIONS #
########################

use Getopt::Long;

# Every test can accept any options, but some options has to be supported.
# Next two options are needed to redirect STDOUT and STDERR after forking the
# process, you'll see in a few line why we're doing it.

my $outputfile = '/var/log/cvmfs-test/edit_repo_basic.out';
my $errorfile = '/var/log/cvmfs-test/edit_repo_basic.err';

# Next option is probably useless as far as I dind't find yet a test that needs
# an unclean environment, but it's better to have it.

my $no_clean = undef;

# After setting all variables, here we're parsing command line options that
# were actually received by the script. Have a look to 'perldoc Getopt::Long'
# for a reference on how tu use this module.
my $ret = GetOptions ( "stdout=s" => \$outputfile,
					   "stderr=s" => \$errorfile,
					   "no-clean" => \$no_clean );
###########					   
# FORKING #
###########

# Once the daemon starts the test, it will wait for test output on a pipe. It means that
# while the pipe is open, the daemon will continue to wait. The pipe will be closed only
# when the test close its STDOUT channel.
# This is why we need a fork and, inside the forked process, we will redirect its STDOUT
# and STDERR to the log file. In this way, the main process will send back only few output
# lines and the daemon will come back as soon as possibile in listening mode.
# This is not mandatory, but it's necessary if you intend to send the daemon others command
# within the test.

my $pid = fork();

###################################
# COMMANDS FOR THE FORKED PROCESS #
###################################

# In this section you'll probably store the actual test.
# To keep this test short, we'll leave a lot of lines commented. The code that can be used will
# have two '##' in front of it.

if (defined($pid) and $pid == 0) {
	# As we already said, the first and most important thing to do, is to redirect
	# STDOUT and STDERR to a log file. We have a function in Tests:Common to do it.
	# The function needs a path to the output file and a path to the error file. We set
	# it as variables at the beginning of this script.
	
	set_stdout_stderr($outputfile, $errorfile);
	
	# If we want be able to speak with the daemon, we need to open the socket to
	# communicate with it. We have a function for this, too.
	# The function needs to know the test name to set the socket identity option.
	# If do not pass any test name, it will not set the identity and you will not
	# be able to retrieve any output from the daemon, as it will not know to whom
	# to send it. The function will return two object: the socket object and the
	# context object. You'll probably will never need the context object if not to close
	# it properly at the end of your script.
	
	my ($socket, $ctxt) = open_test_socket($testname);

	# We cannot use the same socket to speak with the daemon and send output to the shell,
	# so we have to open a new socket. This function return two object: the socket
	# object and the context obkect. This time, we don't need to set any identity for our
	# socket as we don't need to get any data from the shell. We're not commenting it as
	# we'll need to send output to the shell before the end of the script.

	my ($shell_socket, $shell_ctxt) = open_shellout_socket();
	
	# It's a good idea to have your script to work in a clean environment. Every test,
	# before starting, should send to the daemon a 'clean' command.
	
	if (!defined($no_clean)) {
		print "\nCleaning the environment:\n";
		$socket->send("clean");
		get_daemon_output($socket);
		sleep 5;
	}
	else {
		print "\nSkipping cleaning.\n";
	}
	
	# Now you want probably to setup the environment.
	# The next function will extract a repository, generate rsa keys and sign files.
	# The functions needs two parameter: the path were to extract the repo and the repo name
	# (required to sign it properly).
	
	setup_environment($tmp_repo, $repo_name);
	
	# The last required action is to properly setup cvmfs to mount the repo.
	# The configuration is accomplished with a shell script inside the directory of
	# this script. The script needs to write some lines on configuration files in /etc,
	# this is why we need to call it with sudo.
	
	##print 'Configuring cvmfs... ';
	##system("sudo $RealBin/cvmfs_config.sh");
	##print "Done.\n";
	
	# Now the setup process is finally complete. We're ready to test out whatever we want.
	# I suggest to use some separator between tests, so you can easily distinguish them
	# in the log file.
	
	print '-'x30 . 'FIRST_TEST' . '-'x30 . '\n';
	
	# At this point, we can start as many services we want, just use the same syntax we
	# used to send the 'clean' command.
	
	# Every services will return a code with its PID. You will get this code when reading
	# daemon's output. This is why you should declare and array where to store all this pids.
	
	my @pids = undef;
	
	# This array must be passed as argument of the get_daemon_output() functions. It's not only
	# the argument,  but the return value of the function must be assigned again to this array.
	# Have a look to the following code.
	
	print "Starting services for mount_successfull test...\n";
	$socket->send("httpd --root $repo_pub --index-of --all --port 8080");
	@pids = get_daemon_output($socket, @pids);
	sleep 5;
	print "Done.\n";
	
	# Executing the whole code above, the repo will be accessible in /cvmfs/$repo_name.
	# We can check it with the check_repo function.
	
	if(check_repo("/cvmfs/$repo_name")) {
		print "Mounted.\n";
	}
	
	# Almost everything is said now.
	# There's a last important part: the output from the test.
	# We have a socket object stored in the variable $shell_socket and we have a method called 'send'
	# to send message through this socket.
	
	# We're not commenting these lines since they need very little time and since we need to send
	# something to shell after it receives the READ_RETURN_CODE signal, otherwise it will hang forever.
	
	$shell_socket->send("This is just a test output.\n");
	$shell_socket->send("Shell will print in green line with a capitalized OK.\n");
	$shell_socket->send("Shell will print in red lines with a capitalized WRONG.\n");
	$shell_socket->send("You could find all skeleton file to build a test in $RealBin.\n");
	
	# When we are finished with the output, we have to send the shell an ending message.
	# This is not different from sending it another message, just the content has to be "END\n".
	# If we forgot to do it, the shell will continue waiting for test output and will hang forever.

	$shell_socket->send("END\n");	
	
	# As last thing, when your script is finished, you need to close the socket and the context of both
	# daemon and shell socket.
	# This is to avoid future crash for multiple socket connecting simultaneusly.

	##close_test_socket($socket, $ctxt);
	close_test_socket($shell_socket, $shell_ctxt);
}

#################################
# COMMANDS FOR THE MAIN PROCESS #
#################################

# The daemon will wait for everything that starts below this point.
# Until now, I had the need to add only few prints line. Everything you print
# here will be printed on STDOUT (that is a pipe). The daemon will read this
# pipe and will send every single line back to the shell.

if (defined($pid) and $pid != 0) {
	print "$testname test started.\n";
	print "You can read its output in $outputfile.\n";
	print "Errors are stored in $errorfile.\n";
	
	# Next line is needed to communicate to the shell that a process is still
	# running (the forked one). Once the shell reads this line, it will start
	# printing a loading animation. Some test will need very long time before
	# producing any output. Adding this line is always a good idea if you want
	# to avoid that the shell seems to hang.
	# As convention, lines starting with only capitalized letter are read from the
	# shell as special command that must be somehow processed.
	print "PROCESSING:$testname\n";
	
	# Next line is needed to tell the shell that an output is going to
	# be sent by the started test. Once the shell will read this special command
	# it will start waiting for an output.
	# Be careful because the shell will wait for this output and if you don't
	# print any output to it and if you forget about the ending message,
	# the shell will hang forever after receiving this signal.
	print "READ_RETURN_CODE";
}

exit 0;
