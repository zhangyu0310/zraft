#!/bin/bash

protoc --go_out=./rpc --go-grpc_out=./rpc ./pb/raft.proto
