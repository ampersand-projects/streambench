#!/bin/bash -e

function run {
    pushd $1
    ./run.sh
    popd
}

run trill_bench
run streambox_bench
run grizzly_bench
run lightsaber2_bench
run tilt_bench
