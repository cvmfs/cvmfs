# Documentation: https://github.com/Homebrew/homebrew/blob/master/share/doc/homebrew/Formula-Cookbook.md
#                http://www.rubydoc.info/github/Homebrew/homebrew/master/Formula
# PLEASE REMOVE ALL GENERATED COMMENTS BEFORE SUBMITTING YOUR PULL REQUEST!

class Cvmfs < Formula
  desc "HTTP File System for globally Distributing Software."
  homepage "http://cernvm.cern.ch/portal/filesystem"
  url "file:///Users/rmeusel/Documents/CERN/cvmfs/dist/source.tar.gz"
  sha256 ""
  version "2.2.0-0"
  head "https://github.com/cvmfs/cvmfs.git"

  depends_on "cmake" => :build
  depends_on "openssl"
  depends_on :osxfuse

  def install
    # Remove unrecognized options if warned by configure
    system "cmake", "-DCMAKE_INSTALL_PREFIX=${prefix}", ".", *std_cmake_args
    system "make"
    system "make", "install"
  end

  def caveats; <<-EOS.undent
    To finish installation you will need to do two more steps:
      1. run `sudo cvmfs_config setup`
      2. create /etc/cvmfs/default.local and configure at least an HTTP proxy
         CVMFS_HTTP_PROXY=<your site-local proxy | or 'DIRECT' for no proxy>

    Afterwards you can mount CernVM-FS repositories like this:
      sudo mkdir -p /cvmfs/atlas.cern.ch
      sudo mount -t cvmfs atlas.cern.ch /cvmfs/atlas.cern.ch 
    EOS
  end

  test do
    # `test do` will create, run in and delete a temporary directory.
    #
    # This test will fail and we won't accept that! It's enough to just replace
    # "false" with the main program this formula installs, but it'd be nice if you
    # were more thorough. Run the test with `brew test cvmfs`. Options passed
    # to `brew install` such as `--HEAD` also need to be provided to `brew test`.
    #
    # The installed folder is not in the path, so use the entire path to any
    # executables being tested: `system "#{bin}/program", "do", "something"`.
    system "false"
  end
end
