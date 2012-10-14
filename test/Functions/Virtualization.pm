package Functions::Virtualization;

########################################
# Here will be stored all function needed to set up a distributed environment.
########################################

use strict;
use warnings;
use FindBin qw($RealBin);
use Tests::Common qw(recursive_rm);
use Getopt::Long;
use Sys::Detect::Virtualization;
use File::Copy;
use LWP::Simple;

my $distributed = "$RealBin/Distributed";
my $home = $ENV{"HOME"};
my $cernvmurl = 'http://cernvm.cern.ch/releases/17/cernvm-basic-2.6.0-4-1-x86_64.vdi.gz';
my $cernvmgz = 'cernvm-basic-2.6.0-4-1-x86_64.vdi.gz';
my $cernvmvdi = 'cernvm-basic-2.6.0-4-1-x86_64.vdi';
my $vmname = 'CVMFSTEST';
my $vbm = '/usr/bin/VBoxManage';

# Next lines are needed to export subroutine to the main package
use base 'Exporter';
use vars qw/ @EXPORT_OK /;
@EXPORT_OK = qw(start_distributed stop_vmtest);

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

# Next funxtion checks if $vbm is installed
sub check_vbm {
	if (-f '$vbm') {
		return 1;
	}
	else {
		print "ERROR: It seems you don't have $vbm on your system. Do you have VirtualBox installed?\n";
		return 0;
	}
}

# Next funxtion checks if the virtual machine $vmname already exists
sub check_vmtest {
	my $exist = `$vbm list vms | grep $vmname`;
	
	if ($exist) {
		return 1;
	}
	else {
		return 0;
	}
}

# Next function will download CernVM image for VirtualBox
sub download_cernvm {
	my $http_code = getstore('http://cernvm.cern.ch/releases/17/$cernvmgz', "$distributed/$cernvmgz");
	
	if ($http_code == 200) {
		return 1;
	}
	else {
		print "\nERROR: Something went wrong while trying to download CernVM image for VirtualBox.\n";
		return 0;
	}
}

# This function will create the file to import ssh keys to newly generated machine
sub in_context {
	mkdir("$distributed/iso");
	
	print 'Generating RSA keys... ';
	system("ssh-keygen -q -t rsa -N \"\" -f $home/.ssh/id_rsa_$vmname");
	print "Done.\n";
	
	print 'Copying RSA key... ';
	copy("$home/.ssh/id_rsa_$vmname.pub", "$distributed/iso/root.pub");
	print "Done.\n";
	
	print 'Generating context.sh file... ';
	open(my $contextsh, '>', "$distributed/iso/context.sh");
	print $contextsh 'ROOT_PUBKEY=root.pub';
	close($contextsh);
	print "Done.\n";
	
	print 'Creating prolog.sh... ';
	open(my $prologsh, '>', "$distributed/iso/prolog.sh");
	close($prologsh);
	print "Done.\n";
	
	print 'Generating context.iso... ';
	system("mkisofs -o $distributed/context.iso $distributed/iso");
	print "Done.\n";
	
	print 'Removing $distributed/iso directory... ';
	recursive_rm("$distributed/iso");
	print "Done.\n";		
}

# This function will create the virtualbox machine
sub vbox_create_vmtest {
	system("$vbm createvm --name $vmname --ostype Linux26_64");
	system("$vbm storagectl $vmname --name \"Controller IDE\" --add ide");
	system("$vbm storageattach $vmname --storagectl \"Controller IDE\" --port 1 --device 0 --type hdd --medium $distributed/$cernvmvdi --mtype normal");
	system("$vbm storageattach $vmname --storagectl \"Controller IDE\" --port 1 --device 1 --type dvddrive --medium $distributed/context.iso");
	system("$vbm modifyvm $vmname --nic2 hostonly");
}

# This fuction will start the virtual machine
sub start_vmtest {
	system("$vbm startvm $vmname --type headless");
}

# This function will shutdown the virtual machine
sub stop_vmtest {
	system("$vbm controlvm $vmname acpipowerbutton");
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
		print "It seems you're on a virtual machine already.\n";
		print "I won't boot a virtual machine inside another one...\n";
		print "However, if you really want to try, you can use the --force option.\n";
		return 0;
	}
	else {
		if (!check_vbm()) {
			return 0;
		}
		
		# Checking if the cernvm image already exists
		if (!-f "$distributed/$cernvmvdi") {
			print "This is the first time you're running the distributed test.\n";
			print "At the end of the setup process, you'll have new virtual machine called $vmname in your VirtualBox profile.\n";
			print "The preparation of the test will take a lot of time...\n";
			print "Downloading CernVM image for VirtualBox...";
			if (!download_CernVM()) {
				return 0;
			}
			else {
				print "Done.\n";
			}
			
			# Extracting CernVM
			print "Extracting CernVM image from $distributed/cernvm.vdi.gz... ";
			system("tar -xzf $distributed/$cernvmgz -C $distributed");
			print "Done.\n";
			
			# Erasing archive
			print 'Erasing $distributed/cernvm.vdi.gz... ';
			unlink("$distributed/$cernvmgz");
			print "Done.\n";
			
			# Generating the contextualization iso
			in_context();
		}
		
		# Generating the VirtualBox machine
		if (!check_vmtest()) {
			vbox_create_vmtest();
		}
		
		start_vmtest();
	}
}

1;
