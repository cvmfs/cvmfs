# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure(2) do |config|
  config.vm.box = "cernvm"
  # config.vm.box_url = ... TODO(reneme): maybe add later for convenience

  config.vm.boot_timeout = 1200 # CernVM might load stuff over a slow network
                                # and need a lot of time on first boot up

  config.vm.network "private_network", ip: "192.168.33.10"

  # config.vm.synced_folder '.', '/vagrant', nfs: true   TODO(reneme): quicker!

  config.vm.provision "shell", path: "vagrant/provision_cernvm.sh"

  # allow virtual box to take advantage of the host's speed
  # Snippet found here (thanks to Stefan Wrobel): 
  # https://stefanwrobel.com/how-to-make-vagrant-performance-not-suck
  config.vm.provider "virtualbox" do |v|
    host = RbConfig::CONFIG['host_os']

    # Give VM 1/4 system memory & access to all cpu cores on the host
    if host =~ /darwin/
      cpus = `sysctl -n hw.ncpu`.to_i
      # sysctl returns Bytes and we need to convert to MB
      mem = `sysctl -n hw.memsize`.to_i / 1024 / 1024 / 4
    elsif host =~ /linux/
      cpus = `nproc`.to_i
      # meminfo shows KB and we need to convert to MB
      mem = `grep 'MemTotal' /proc/meminfo | sed -e 's/MemTotal://' -e 's/ kB//'`.to_i / 1024 / 4
    else # sorry Windows folks, I can't help you
      cpus = 2
      mem = 1024
    end

    v.customize ["modifyvm", :id, "--memory", mem]
    v.customize ["modifyvm", :id, "--cpus", cpus]
  end
end
