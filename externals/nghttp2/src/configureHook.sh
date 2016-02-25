#!/bin/sh

autoreconf -i
automake
autoconf
./configure
