
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

  def test
    system bin/"cvmfs2", "--version"

    # brew runs tests inside a `sandbox-exec` environment which prevents us
    # from mounting a FUSE-based file system. If the test case is run with
    # `brew test --no-sandbox cvmfs` the commented code below works fine.
    #
    # TODO(rmeusel): How to tell `brew` to not run this test in sandbox-exec
    #                --> enable the more sophisticated test later
    #
    # cfg_file   = testpath/"private_mount.conf"
    # cache_dir  = testpath/"cache"
    # mountpoint = testpath/"mountpoint"
    # Dir.mkdir "#{cache_dir}"
    # Dir.mkdir "#{mountpoint}"
    # cfg_file.write <<-EOS.undent
    #   CVMFS_SERVER_URL=http://cvmfs-stratum-one.cern.ch/cvmfs/@fqrn@
    #   CVMFS_HTTP_PROXY=DIRECT
    #   CVMFS_CACHE_BASE=#{cache_dir}
    #   CVMFS_RELOAD_SOCKETS=#{cache_dir}
    #   CVMFS_PUBLIC_KEY=#{etc}/cvmfs/keys/cern.ch/cern.ch.pub
    # EOS
    # system "cvmfs2", "-o", "config=#{cfg_file}", "atlas.cern.ch", "#{mountpoint}"
    # Timeout.timeout(30) do
    #   while true do
    #     if Dir.entries(mountpoint).length > 2
    #       break
    #     else
    #       sleep(1)
    #     end
    #   end
    # end
    # system "umount", "#{mountpoint}"
  end
end
