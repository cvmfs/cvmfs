#!/bin/bash

docker run --rm -it -v "${PWD}":/mnt radupopescu/erlang-libsodium:19 \
       sh -c "cd /mnt && rebar3 release && rebar3 as test dialyzer,ct && rebar3 as prod tar"
