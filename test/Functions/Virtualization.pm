package Functions::Virtualization;

########################################
# Here will be stored all function needed to set up a distributed environment.
########################################

use strict;
use warnings;
use FindBin($RealBin);
use Tests::Common qw(recursive_rm);
use Getopt::Long;
use Sys::Detect::Virtualization;
use File::Copy;
use LWP::Simple;

# Next lines are needed to export subroutine to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(start_distributed);

# Next function will detect if cvmfs-test is running on a VM. It returns true or false.
sub detect_virtualization {
    my $detector = Sys::Detect::Virtualization->new();

    my @found = $detector->detect();
    if( @found ) {
        return 1;
    }
    else {
		return 0;
	}
}

# Next funxtion checks if VBoxManage is installed
sub check_VBoxManage {
	if (-f '/usr/bin/VBoxManage') {
		return 1;
	}
	else {
		print "ERROR: It seems you don't have /usr/bin/VBoxManage on your system. Do you have VirtualBox installed?\n";
		return 0;
	}
}

# Next function will download CernVM image for VirtualBox
sub download_CernVM {
	my $http_code = getstore('http://cernvm.cern.ch/releases/17/cernvm-basic-2.6.0-4-1-x86_64.vdi.gz', "$RealBin/Distributed/cernvm.vdi.gz");
	
	if ($http_code = 200) {
		return 1;
	}
	else {
		print "\nERROR: Something went wrong while trying to download CernVM image for VirtualBox.\n"
		return 0;
	}
}

# This function will create the file to import ssh keys to newly generated machine
sub in_context {
	mkdir("$RealBin/Distributed/iso");
	
	my $home = $ENV{"HOME"};
	
	print 'Generating RSA keys... ';
	system("ssh-keygen -q -t rsa -N \"\" -f $home/.ssh/id_rsa_cvmfstest");
	print "Done.\n";
	
	print 'Copying RSA key... '
	copy("$home/.ssh/id_rsa_cvmfstest.pub", "$RealBin/Distributed/iso/root.pub");
	print "Done.\n";
	
	print 'Generating context.sh file... ';
	open(my $contextsh, '>', "$RealBin/Distributed/iso/context.sh");
	print $contextsh 'ROOT_PUBKEY=root.pub';
	close($contextsh);
	print "Done.\n";
	
	print 'Creating prolog.sh... ';
	open(my $prologsh, '>', "$RealBin/Distributed/iso/prolog.sh");
	close($prologsh);
	print "Done.\n";
	
	print 'Generating context.iso... ';
	system("mkisofs -o $RealBin/Distributed/context.iso $RealBin/Distributed/iso");
	print "Done.\n";
	
	print 'Removing $RealBin/Distributed/iso directory... ';
	recursive_rm("$RealBin/Distributed/iso");
	print "Done.\n";		
}

# This function will create the virtualbox machine
sub vbox_create_cvmfstest {
	system("VBoxManage createvm --name CVMFSTEST --ostype Linux26_64");
	system("VBoxManage storagectl CVMFSTEST --name \"Controller IDE\" --add ide");
	system("VBoxManage storageattach CVMFSTEST --storagectl \"Controller IDE\" --port 1 --device 0 --type hdd --medium $RealBin/Distributed/cernvm.vdi --mtype normal");
	system("VBoxManage storageattach CVMFSTEST --storagectl \"Controller IDE\" --port 1 --device 1 --type dvddrive --medium $RealBin/Distributed/context.iso");
	system("VBoxManage modifyvm CVMFSTEST --nic2 hostonly");
}

# The next function will actually execute every thing that is needed to start the virtual machine
sub start_distributed {
	# Retrieving all parameters
	@ARGV = @_;
	
	my $distributed = undef;
	my $daemon_output = undef;
	my $daemon_error = undef;
	my $force = undef;
	
	my $ret = GetOptions ( "distributed" => \$distributed,
						   "stdout=s" => \$daemon_output,
						   "stderr=s" => \$daemon_error,
						   "force" => \$force );
	
	if (detect_virtualization() and !defined($force)) {
		# Exiting if we're running on a virtual machine
		print "It seems you're on a virtual machine already.\n"
		print "I won't boot a virtual machine inside another one...\n";
		print "However, if you really want to try, you can use the --force option.\n";
		return 0;
	}
	else {
		if (!check_VBoxManage()) {
			return;
		}
		
		# Checking if the cernvm image already exists
		if (!-f "$RealBin/Distributed/cernvm.vdi") {
			print "This is the first time you're running the distributed test.\n";
			print "The preparation of the test will take a lot of time...\n";
			print "Downloading CernVM image for VirtualBox...";
			if (!download_CernVM()) {
				return;
			}
			else {
				print "Done.\n";
			}
		}
		
		# Extracting CernVM
		print "Extracting CernVM image from $RealBin/Distributed/cernvm.vdi.gz... ";
		system("tar -xzf $RealBin/Distributed/cernvm.vdi.gz -C $RealBin/Distributed");
		print "Done.\n";
		
		# Erasing archive
		print 'Erasing $RealBin/Distributed/cernvm.vdi.gz... ';
		unlink("$RealBin/Distributed/cernvm.vdi.gz");
		print "Done.\n";
		
		# Renaming CernVM image
		system("mv $RealBin/Distributed/cern* cernvm.vdi");
		
		# Generating the contextualization iso
		in_context();
		
		# Generating the VirtualBox machine
		vbox_create_cvmfstest();
	}
}

1;
