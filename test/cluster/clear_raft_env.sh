#!/bin/bash

PATH=`pwd`

echo $PATH/raft_test_0/state_machine
echo $PATH/raft_test_1/state_machine
echo $PATH/raft_test_2/state_machine

/bin/rm -rf $PATH/raft_test_0/state_machine
/bin/rm -rf $PATH/raft_test_1/state_machine
/bin/rm -rf $PATH/raft_test_2/state_machine

/bin/rm $PATH/raft_test_0/zlogger.*
/bin/rm $PATH/raft_test_1/zlogger.*
/bin/rm $PATH/raft_test_2/zlogger.*

/bin/rm zlogger.*
/bin/rm -r state_machine
