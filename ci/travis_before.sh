#!/bin/bash

if [ ! -f ~/bin/rebar3 ]; then
    # Rebar3
    mkdir -p ~/bin
    wget -O ~/bin/rebar3 https://s3.amazonaws.com/rebar3/rebar3
    chmod +x ~/bin/rebar3

    # Libsodium
    wget https://github.com/jedisct1/libsodium/releases/download/1.0.11/libsodium-1.0.11.tar.gz
    tar xzf libsodium-1.0.11.tar.gz && cd libsodium-1.0.11 && ./configure && make && make install
fi

