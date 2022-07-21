#!/bin/bash

ps -ef | grep './cluster -id=' | grep -v grep | awk '{print $2}' | xargs kill
