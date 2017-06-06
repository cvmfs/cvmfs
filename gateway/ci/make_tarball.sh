#!/bin/sh

rebar3 as prod compile
cd _build/prod/lib/syslog
./rebar compile
cd -
rebar3 as prod release,tar
