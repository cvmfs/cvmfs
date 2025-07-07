#!/usr/bin/env python3

from LbCVMFS import Tools

def print_wrapper(msg):
    print "WRAPPER:" , msg

if __name__ == "__main__":
    with Tools.cvmfsTransaction():
        print_wrapper("not installing anything")
