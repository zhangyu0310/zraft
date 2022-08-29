#!/bin/bash

./build.sh

PATH=$(pwd)

cd "$PATH"/raft_test_0 || exit;./cluster -id=0 -size=3 &
cd "$PATH"/raft_test_1 || exit;./cluster -id=1 -size=3 &
cd "$PATH"/raft_test_2 || exit;./cluster -id=2 -size=3 &
