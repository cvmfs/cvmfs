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
      cpus = [`sysctl -n hw.ncpu`.to_i / 2, 1].max
      # sysctl returns Bytes and we need to convert to MB
      mem = `sysctl -n hw.memsize`.to_i / 1024 / 1024 / 4
    elsif host =~ /linux/
      cpus = [`nproc`.to_i / 2, 1].max
      # meminfo shows KB and we need to convert to MB
      mem = `grep 'MemTotal' /proc/meminfo | sed -e 's/MemTotal://' -e 's/ kB//'`.to_i / 1024 / 4
    else # sorry Windows folks, I can't help you
      cpus = 2
      mem = 1024
    end

    v.customize ["modifyvm", :id, "--memory", mem]
    v.customize ["modifyvm", :id, "--cpus", cpus]
  end

  config.vm.define "cernvm2" do |cvm2|
    cvm2.vm.box = "cernvm2"
    cvm2.vm.box_url = "http://cernvm.cern.ch/releases/ucernvm-images.2.6-7.cernvm.x86_64/ucernvm-slc5.2.6-7.cernvm.x86_64.box"

    cvm2.vm.boot_timeout = 1200 # CernVM might load stuff over a slow network
                                  # and need a lot of time on first boot up

    #cvm2.vm.network "private_network", ip: "192.168.33.10"
    cvm2.vm.network "private_network", type: "dhcp", auto_config: false
    cvm2.vm.synced_folder '.', '/vagrant'

    cvm2.vm.provision "shell", path: "vagrant/provision_cernvm.sh"
  end

  config.vm.define "cernvm" do |cvm|
    cvm.vm.box = "cernvm36"
    cvm.vm.box_url = "http://cernvm.cern.ch/releases/production/cernvm-3.6.5.box"

    cvm.vm.boot_timeout = 1200 # CernVM might load stuff over a slow network
                                  # and need a lot of time on first boot up

    #cvm.vm.network "private_network", ip: "192.168.33.10"
    cvm.vm.network "private_network", type: "dhcp", auto_config: false
    cvm.vm.synced_folder '.', '/vagrant'

    cvm.vm.provision "shell", path: "vagrant/provision_cernvm.sh"
  end

  config.vm.define "cernvm4" do |cvm|
    cvm.vm.box = "cernvm4"
    cvm.vm.box_url = "http://cernvm.cern.ch/releases/ucernvm-images.2.7-7.cernvm.x86_64/ucernvm-sl7.2.7-7.cernvm.x86_64.box"

    cvm.vm.boot_timeout = 1200 # CernVM might load stuff over a slow network
                                  # and need a lot of time on first boot up

    #cvm.vm.network "private_network", ip: "192.168.33.10"
    cvm.vm.network "private_network", type: "dhcp", auto_config: false
    cvm.vm.synced_folder '.', '/vagrant'

    cvm.vm.provision "shell", path: "vagrant/provision_cernvm4.sh"
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
    slc6.vm.synced_folder '.', '/vagrant'

    slc6.vm.provision "shell", path: "vagrant/provision_slc6.sh"
    slc6.vm.provision :reload
  end

  config.vm.define "ubuntu1604" do |ub|
    ub.vm.box = "geerlingguy/ubuntu1604"
    ub.vm.network "private_network", ip: "192.168.33.15"
    ub.vm.synced_folder '.', '/vagrant'
    ub.vm.provision "shell", path: "vagrant/provision_ubuntu.sh"
  end

  config.vm.define "debian8" do |deb|
    deb.vm.box = "debian/jessie64"
    deb.vm.network "private_network", ip: "192.168.33.25"
    deb.vm.provision "shell", path: "vagrant/provision_debian.sh"
  end

  config.vm.define "ubuntu1204" do |ub|
    ub.vm.box = "ubuntu/precise64"
    ub.vm.network "private_network", ip: "192.168.33.16"
    ub.vm.synced_folder '.', '/vagrant'
    ub.vm.provision "shell", path: "vagrant/provision_ubuntu.sh"
  end

  config.vm.define "ubuntu1204-32" do |ub|
    ub.vm.box = "ubuntu/precise32"
    ub.vm.network "private_network", ip: "192.168.33.18"
    ub.vm.synced_folder '.', '/vagrant'
    ub.vm.provision "shell", path: "vagrant/provision_ubuntu.sh"
  end

  config.vm.define "fedora" do |fedora|
    fedora.vm.box = "fedora/23-cloud-base"
    fedora.vm.network "private_network", ip: "192.168.33.13"
    fedora.vm.synced_folder '.', '/vagrant'
    fedora.vm.provision "shell", path: "vagrant/provision_fedora.sh"
  end

  config.vm.define "fedora24" do |fedora|
    fedora.vm.box = "fedora/24-cloud-base"
    fedora.vm.network "private_network", ip: "192.168.33.19"
    fedora.vm.synced_folder '.', '/vagrant'
    fedora.vm.provision "shell", path: "vagrant/provision_fedora.sh"
  end

  config.vm.define "fedora26" do |fedora|
    fedora.vm.box = "fedora/26-cloud-base"
    fedora.vm.network "private_network", ip: "192.168.33.21"
    fedora.vm.provision "shell", path: "vagrant/provision_fedora.sh"
  end

  config.vm.define "centos7" do |centos7|
    centos7.vm.box = "geerlingguy/centos7"
    centos7.vm.network "private_network", ip: "192.168.33.14"
    centos7.vm.synced_folder '.', '/vagrant'
    centos7.vm.provision "shell", path: "vagrant/provision_centos7.sh"
  end

  config.vm.define "arch" do |arch|
    arch.vm.box = "ogarcia/archlinux-x64"
    arch.vm.network "private_network", ip: "192.168.33.20"
    arch.vm.synced_folder '.', '/vagrant'
    arch.vm.provision "shell", path: "vagrant/provision_arch.sh"
  end
end
