#!/bin/bash

PATH=$(pwd)

echo "$PATH"/raft_test_0/state_machine
echo "$PATH"/raft_test_1/state_machine
echo "$PATH"/raft_test_2/state_machine

/bin/rm -rf "$PATH"/raft_test_0/state_machine
/bin/rm -rf "$PATH"/raft_test_1/state_machine
/bin/rm -rf "$PATH"/raft_test_2/state_machine

/bin/rm "$PATH"/raft_test_0/cluster
/bin/rm "$PATH"/raft_test_1/cluster
/bin/rm "$PATH"/raft_test_2/cluster

/bin/rm "$PATH"/raft_test_0/zraft_log
/bin/rm "$PATH"/raft_test_1/zraft_log
/bin/rm "$PATH"/raft_test_2/zraft_log

/bin/rm "$PATH"/raft_test_0/zlogger.*
/bin/rm "$PATH"/raft_test_1/zlogger.*
/bin/rm "$PATH"/raft_test_2/zlogger.*

/bin/rm zlogger.*
/bin/rm -r state_machine
/bin/rm zraft_log
