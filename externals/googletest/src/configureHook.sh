#!/bin/sh

CXXFLAGS="-DGTEST_HAS_TR1_TUPLE=0 -fPIC" cmake .
