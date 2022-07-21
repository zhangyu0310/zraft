#!/bin/bash

PATH=`pwd`

cd $PATH/raft_test_0;./cluster -id=0 -size=3 &
cd $PATH/raft_test_1;./cluster -id=1 -size=3 &
cd $PATH/raft_test_2;./cluster -id=2 -size=3 &
