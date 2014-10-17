#!/usr/bin/python

import boto.ec2
import time
import argparse
import sys


def print_error(msg):
  print >> sys.stderr , "[Error]" , msg


def is_running(instance):
  return instance != None and instance.state == "running"


def wait_for_instance(instance, timeout = 900):
  start = time.time()
  counter = 0
  polling_interval = 15
  while instance.state == "pending" and counter < timeout:
    instance.update()
    time.sleep(polling_interval)
    counter += polling_interval
  end = time.time()
  return end - start


def connect_to_openstack(access_key, secret_key, endpoint):
  region = boto.ec2.regioninfo.RegionInfo(name        = "nova",
                                          endpoint    = endpoint)
  connection = boto.connect_ec2(aws_access_key_id     = access_key,
                                aws_secret_access_key = secret_key,
                                is_secure=False,
                                region=region,
                                port=8773,
                                path="/services/Cloud")
  return connection


def spawn_instance(connection, ami, key_name, flavor, userdata, max_retries = 5):
  instance     = None
  time_backoff = 20
  retries      = 0

  while not is_running(instance) and retries < max_retries:
    instance = spawn_instance_on_openstack(connection,
                                           ami,
                                           key_name,
                                           flavor,
                                           userdata)
    retries += 1
    if not is_running(instance):
      instance_id    = "unknown"
      instance_state = "unknown"
      if instance != None:
        instance_id    = str(instance.id)
        instance_state = str(instance.state)
        kill_instance(connection, instance_id)
      print_error("Failed spawning instance " + instance_id +
                  " (#: " + str(retries) + " | state: " + instance_state + ")")
      time.sleep(time_backoff)

  if not is_running(instance):
    return None
  return instance


def spawn_instance_on_openstack(connection, ami, key_name, flavor, userdata):
  try:
    reservation = connection.run_instances(ami,
                                           key_name=key_name,
                                           instance_type=flavor,
                                           user_data=userdata)
    if len(reservation.instances) != 1:
      print_error("Failed to start instance (#: " + reservation.instances + ")")
      return None

    instance = reservation.instances[0]
    if instance.state != "pending":
      print_error("Instance failed at startup (State: " + instance.state + ")")
      return instance

    waiting_time = wait_for_instance(instance)
    if instance.state != "running":
      print_error("Failed to boot up instance (State: " + instance.state + ")")
      return instance

    return instance

  except Exception, e:
    print_error("Exception: " + str(e))
    return None


def kill_instance(connection, instance_id):
  terminated_instances = []
  try:
    terminated_instances = connection.terminate_instances(instance_id)
  except Exception, e:
    print_error("Exception: " + str(e))
  return len(terminated_instances) == 1


def create_instance(parent_parser, argv):
  parser = argparse.ArgumentParser(parents=[parent_parser])
  parser.add_argument("--ami",
                         nargs    = 1,
                         metavar  = "<ami_name>",
                         required = True,
                         dest     = "ami",
                         help     = "AMI identifier of the image to boot")
  parser.add_argument("--key",
                         nargs    = 1,
                         metavar  = "<key_name>",
                         required = True,
                         dest     = "key",
                         help     = "Name of the access key to use")
  parser.add_argument("--instance-type",
                         nargs    = 1,
                         metavar  = "<instance_type>",
                         required = True,
                         dest     = "flavor",
                         help     = "VM flavor to use")
  parser.add_argument("--userdata",
                         nargs    = 1,
                         metavar  = "<user_data>",
                         required = False,
                         dest     = "userdata",
                         default  = "",
                         help     = "Cloud-init user data string")
  arguments = parser.parse_args(argv)

  access_key = arguments.access_key[0]
  secret_key = arguments.secret_key[0]
  endpoint   = arguments.cloud_endpoint[0]
  ami        = arguments.ami[0]
  key_name   = arguments.key[0]
  flavor     = arguments.flavor[0]
  userdata   = arguments.userdata[0]

  connection = connect_to_openstack(access_key, secret_key, endpoint)
  instance   = spawn_instance(connection, ami, key_name, flavor, userdata)

  if is_running(instance):
    print instance.id , instance.private_ip_address
  else:
    print_error("Failed to start instance")
    exit(2)


def terminate_instance(parent_parser, argv):
  parser = argparse.ArgumentParser(parents=[parent_parser])
  parser.add_argument("--instance-id",
                         nargs    = 1,
                         metavar  = "<instance_id>",
                         required = True,
                         dest     = "instance_id",
                         help     = "Instance ID of the instance to terminate")
  arguments = parser.parse_args(argv)

  access_key  = arguments.access_key[0]
  secret_key  = arguments.secret_key[0]
  endpoint    = arguments.cloud_endpoint[0]
  instance_id = arguments.instance_id[0]

  connection = connect_to_openstack(access_key, secret_key, endpoint)
  successful = kill_instance(connection, instance_id)

  if not successful:
    print_error("Failed to terminate instance")
    exit(2)


#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#

parser = argparse.ArgumentParser(add_help    = False,
                                 description = "Start an Ibex instance")
parser.add_argument("--access-key",
                       nargs    = 1,
                       metavar  = "<aws_access_key_id>",
                       required = True,
                       dest     = "access_key",
                       help     = "EC2 Access Key String")
parser.add_argument("--secret-key",
                       nargs    = 1,
                       metavar  = "<aws_secret_access_key>",
                       required = True,
                       dest     = "secret_key",
                       help     = "EC2 Secret Key String")
parser.add_argument("--cloud-endpoint",
                       nargs    = 1,
                       metavar  = "<url to cloud controller endpoint>",
                       required = True,
                       dest     = "cloud_endpoint",
                       help     = "URL to the cloud controller")

if len(sys.argv) < 2:
    print_error("please provide 'spawn' or 'terminate' as a subcommand...")
    exit(1)

subcommand = sys.argv[1]
argv       = sys.argv[2:]
if   subcommand == "spawn":
  create_instance(parser, argv)
elif subcommand == "terminate":
  terminate_instance(parser, argv)
else:
  print_error("unrecognized subcommand '" + subcommand + "'")
  exit(1)
