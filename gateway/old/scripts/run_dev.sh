#!/bin/sh

rebar3 auto --config config/sys.config.dev --sname cvmfs --setcookie cvmfs --apps cvmfs_gateway,runtime_tools,sasl
