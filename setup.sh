function setup {
    pushd $1
    ./setup.sh
    popd
}

setup trill_bench
setup streambox_bench
setup grizzly_bench
setup lightsaber2_bench
setup tilt_bench
