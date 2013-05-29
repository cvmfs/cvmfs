#!/usr/bin/python

import boto.ec2
import time
import argparse
import sys


def print_error(msg):
  print >> sys.stderr , "[Error]" , msg


def wait_for_instance(instance):
  start = time.time()
  while instance.state == "pending":
    instance.update()
    time.sleep(15)
  end = time.time()
  return end - start


def connect_to_ibex(access_key, secret_key, endpoint):
  region = boto.ec2.regioninfo.RegionInfo(name        = "nova",
                                          endpoint    = endpoint)
  connection = boto.connect_ec2(aws_access_key_id     = access_key,
                                aws_secret_access_key = secret_key,
                                is_secure=False,
                                region=region,
                                port=8773,
                                path="/services/Cloud")
  return connection


def spawn_instance(connection, ami, key_name, flavor):
  try:
    reservation = connection.run_instances(ami,
                                           key_name=key_name,
                                           instance_type=flavor)
    if len(reservation.instances) != 1:
      print_error("Failed to start instance")
      return None

    instance = reservation.instances[0]
    if instance.state != "pending":
      print_error("Instance ended up in an unexpected state: " + instance.state)
      return None

    waiting_time = wait_for_instance(instance)
    if instance.state != "running":
      print_error("Failed to boot up instance")
      return None

    return instance

  except Exception, e:
    print e
    return None


def kill_instance(connection, instance_id):
  terminated_instances = []
  try:
    terminated_instances = connection.terminate_instances(instance_id)
  except Exception, e:
    print e
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
  arguments = parser.parse_args(argv)

  access_key = arguments.access_key[0]
  secret_key = arguments.secret_key[0]
  endpoint   = arguments.cloud_endpoint[0]
  ami        = arguments.ami[0]
  key_name   = arguments.key[0]
  flavor     = arguments.flavor[0]

  connection = connect_to_ibex(access_key, secret_key, endpoint)
  instance   = spawn_instance(connection, ami, key_name, flavor)

  if instance != None:
    print instance.id , instance.ip_address
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

  connection = connect_to_ibex(access_key, secret_key, endpoint)
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
