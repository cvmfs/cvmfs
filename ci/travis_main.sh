#!/bin/bash

docker run --rm -it -v "${PWD}":/mnt radupopescu/erlang-libsodium:19 sh -c "cd /mnt && rebar3 dialyzer && rebar3 ct"
