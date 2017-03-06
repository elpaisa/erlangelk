#!/usr/bin/env bash

function recompile() {
    rebar3 compile
    rebar3 shell --name "utility1" --setcookie "dssaasdas23214asddfASDsaadsfsder3e324"
}

pkill -f erl
recompile