 + ----------------------------------
 | Cloud Testing

The scripts in this directory are used for automatic multi-platform tests based
on ad-hoc virtual machines. They spawn virtual machines on CERN's in-house cloud
and configure them for CernVM-FS test execution. After a test run on a specific
platform finished successfully, the virtual machine is discarded.
The main use case for the cloud testing scripts is automated tests of nightly
builds.

 
 + ----------------------------------
 | Participating Scripts

There are several scripts, that steer the process of automated cloud testing.
Have a look into the cernvm/ci-scripts repository on GitHub:

  -> cvmfs/cloud_testing/run.sh
   This script is invoked by the user (or the continuous integration system) and
   steers the whole process of creation, configuration, usage and destruction of
   various virtual machine platforms.
   A user provides run.sh with information about the desired test platform as
   well as the CernVM-FS packages to be tested on this platform.

  -> cvmfs/cloud_testing/remote_setup.sh
   The remote_setup.sh script is transferred to a fresh virtual machine and exe-
   cuted. It takes care of some environment preparations on the test platform.
   These include the creation of a test workspace, a test user account (by de-
   fault 'sftnight'), the download of the provided packages and other general
   system configuration.
   Eventually remote_setup.sh will invoke a platform specific setup script that
   takes care of platform specific configurations (see platforms/*).

  -> cvmfs/cloud_testing/remote_run.sh
   After run.sh successfully executed remote_setup.sh on the virtual machine it
   will transfer and run remote_run.sh which takes care of the actual test exe-
   cution. remote_run.sh will do some general sanity checks and then run a plat-
   form specific script (see platforms/*).

  -> cvmfs/cloud_testing/instance_handler.py
   This python script steers the creation and destruction of virtual machines on
   CERN's in-house cloud platform.

Please refer to the scripts itself for further details of their desired usage as
well as their needed parameters.

Platform specific scripts can be found in the platforms/ directory. For each
supported test platform there is both a dedicated 'setup' and 'run' script that
take care of configuration and running of the CernVM-FS test suite.
Setup scripts must install and configure all needed dependencies including the 
subset of provided CernVM-FS packages needed for the test on their platform. Not
all platforms are suitable for server test-cases, for example. Furthermore they
are allowed to reboot the entire machine as their last action, run.sh will take
care of re-establishing the connection.

The platform specific run scripts eventually invoke CernVM-FS's test cases suit-
able for their platform. Usually they first run the unit-tests followed by the
integration tests.


 + ----------------------------------
 | Log Files

All scripts exclusively write to pre-defined log files, that can be downloaded
by the test steering script after the test run has finished. Thus, all outputted
information can be extracted after the test-run. Additionally run.sh will not
destruct the virtual machine in case of test failures for further post-mortem
analysis by hand.

 -> $setup_log
  This should contain the outputs of the setup process steered by the remote
  setup script.

 -> $run_log
  This contains the outputs of steering/remote_run.sh and can be seen as a
  summary of the test run.

 -> $test_log
  Here you find the detailed integration test log file. It contains outputs of
  everything the integration tests invoked.

 -> $unittest_log
  Detailed unit test results end up in this log file.


 + ----------------------------------
 | Flow

   +-----------+
   | run.sh    |
   +-----------+
         |                                +-------------------+
         +------------- Spawn ----------> | Virtual Machine   |
         |      (instance_handler.py)     +-------------------+
         |                                          /
         |                                          \
         +---+--- run remote_setup.sh ------------> /
         |   |                                      \
         |   +--- run platforms/*_setup.sh -------> /
         |                                          \
         |                                          /
         +---+--- run remote_run.sh --------------> \
         |   |                                      /
         |   +-+- run platforms/*_run.sh ---------> \
         |     |                                    /
         |     +----- execute unit tests ---------> \
         |     |                                    /
         |     +----- execute integration tests---> \
         |                                          /
         |                                          \
         +------- retrieve test log files --------> /
         |<---------------------------------------- \
         |                                          /
         |                                          \
         +-------------- Terminate ---------------> /
         |                                         o
         o

