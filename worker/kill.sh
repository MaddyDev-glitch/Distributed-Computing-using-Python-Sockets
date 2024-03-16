#!/bin/bash

PID=$(lsof -ti:12345)

if [ ! -z "$PID" ]; then
    kill -9 $PID
    echo "process killed"
else
    echo "no process found"

fi