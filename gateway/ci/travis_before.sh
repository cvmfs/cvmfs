#!/bin/bash

if [ ! -f ~/bin/rebar3 ]; then
    mkdir -p ~/bin
    wget -O ~/bin/rebar3 https://s3.amazonaws.com/rebar3/rebar3
    chmod +x ~/bin/rebar3
fi

