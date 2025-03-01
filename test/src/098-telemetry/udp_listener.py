import socket
import sys
import getopt

if __name__ == "__main__":
  USAGE = """Simple UDP listener 
  -d
  --destination = hostname/ip of destination

  -p
  --port = port of destination

  -m
  --max-packages = maximum number of packages to receive

  -i
  --must-include = packages must include this string to be accepted, others are ignored
  
  -h
  --help shows this help

  Example: python udp_listener.py --destination=localhost --port=8092 --max-packages=3
  """

  options, arguments = getopt.getopt(
        sys.argv[1:],
        'd:p:m:i:h',
        ["destination=", "port=", "max-packages=", "must-include=", "help="])
  separator = "\n"

  must_include = ""

  for o, a in options:
      if o in ("-d", "--destination"):
          host = a
          # print("Host: " + host)
      if o in ("-p", "--port"):
          port = int(a)
          # print("Port: " + str(port))
      if o in ("-m", "--max-packages"):
          max_packages = int(a)
          # print("Max packages to receive: " + str(max_packages))
      if o in ("-i", "--must-include"):
          must_include = a
          # print("Package must include string: " + must_include)
      if o in ("-h", "--help"):
          print(USAGE)
          sys.exit()
  if not options or len(options) > 4:
      raise SystemExit(USAGE)
  try:
      operands = [int(arg) for arg in arguments]
  except ValueError:
      raise SystemExit(USAGE)

  try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  except socket.error:
    print('Failed to create socket')
    sys.exit()

  s.bind((host, port))

  # endtime = time.time() + 15.0 #sec
  # while time.time() < endtime:

  received_packages = 0

  while received_packages < max_packages:
  #Now receive data
    reply = s.recv(8192)
    if must_include in reply.decode():
      print(reply.decode())
      received_packages += 1
