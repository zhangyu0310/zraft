#!/bin/bash

# shellcheck disable=SC2009
ps -ef | grep './cluster -id=' | grep -v grep | awk '{print $2}' | xargs kill
