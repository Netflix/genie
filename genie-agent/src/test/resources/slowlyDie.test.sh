#!/usr/bin/env bash
#
# Start a process that can only be killed by 3 consecutive Ctrl-C
# or wait for 45s to finish
#

runfile=$RUNFILE
if [ -z "$runfile" ]; then
  runfile=$1
fi
bail() {
    echo hey...
    sleep 15
    echo ok...
    sleep 15
    echo exiting...
    sleep 15
    echo ...exited
    rm $runfile

    trap - SIGINT SIGTERM SIGKILL
    kill -- -$$
}

if [ ! -z "$runfile" ]; then
    touch $runfile || exit 1
fi

trap bail SIGTERM SIGINT SIGKILL

echo "running..."
sleep 50
