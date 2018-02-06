#!/bin/sh

rebar3 auto --config config/sys.config.dev --sname cvmfs --setcookie cvmfs --apps cvmfs_services,runtime_tools,sasl
