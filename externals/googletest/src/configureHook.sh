#!/bin/sh

CXXFLAGS="-DGTEST_USE_OWN_TR1_TUPLE=1" cmake .
