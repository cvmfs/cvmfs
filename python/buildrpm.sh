#!/bin/sh

python setup.py bdist_rpm --requires 'python-requests >= 1.1.0,python-dateutil >= 1.4.1'
