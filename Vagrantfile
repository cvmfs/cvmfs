# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure(2) do |config|
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

  config.vm.define "cernvm" do |cvm|
    cvm.vm.box = "cernvm"
    # cvm.vm.box_url = ... TODO(reneme): maybe add later for convenience

    cvm.vm.boot_timeout = 1200 # CernVM might load stuff over a slow network
                                  # and need a lot of time on first boot up

    cvm.vm.network "private_network", ip: "192.168.33.10"

    # cvm.vm.synced_folder '.', '/vagrant', nfs: true   TODO(reneme): quicker!

    cvm.vm.provision "shell", path: "vagrant/provision_cernvm.sh"
  end

  config.vm.define "slc6" do |slc6|
    unless Vagrant.has_plugin?("vagrant-reload")
      puts "-------------------- WARNING --------------------"
      puts "Vagrant plugin 'vagrant-reload' is not installed."
      puts "Please run: vagrant plugin install vagrant-reload"
      puts "-------------------------------------------------"
    end

    slc6.vm.box = "bytepark/scientific-6.5-64"
    slc6.vm.network "private_network", ip: "192.168.33.11"
    slc6.vm.synced_folder '.', '/vagrant', nfs: true

    slc6.vm.provision "shell", path: "vagrant/provision_slc6.sh"
    slc6.vm.provision :reload
  end

  config.vm.define "yosemite" do |yosemite|
    yosemite.vm.box = "yosemite"
    yosemite.vm.network "private_network", ip: "192.168.33.15"
    yosemite.vm.synced_folder '.', '/Users/vagrant/cvmfs', nfs: true
    yosemite.vm.provision "shell", path: "vagrant/provision_osx.sh"
  end

  config.vm.define "ubuntu" do |ub|
    ub.vm.box = "ubuntu/wily64"
    ub.vm.network "private_network", ip: "192.168.33.12"
    ub.vm.synced_folder '.', '/vagrant', nfs: true
    ub.vm.provision "shell", path: "vagrant/provision_ubuntu.sh"
  end

  config.vm.define "fedora" do |fedora|
    fedora.vm.box = "box-cutter/fedora22"
    fedora.vm.network "private_network", ip: "192.168.33.13"
    fedora.vm.synced_folder '.', '/vagrant', nfs: true

    fedora.vm.provision "shell", path: "vagrant/provision_fedora.sh"

    config.vm.provider "virtualbox" do |vb|
      vb.name = "cvmfs_fedora"
      # Get disk path
      line = `VBoxManage list systemproperties | grep "Default machine folder"`
      vb_machine_folder = line.split(':')[1].strip()
      second_disk = File.join(vb_machine_folder, vb.name, 'disk2.vdi')

      # Create and attach disk
      unless File.exist?(second_disk)
        vb.customize ['createhd', '--filename', second_disk, '--format', 'VDI',
                      '--size', 10 * 1024]
      end
      vb.customize ['storageattach', :id, '--storagectl', 'IDE Controller',
                    '--port', 0, '--device', 1, '--type', 'hdd',
                    '--medium', second_disk]
    end
  end
end
