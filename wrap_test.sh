#!/bin/bash

source test_env.sh;

CMD="python sleep.py"

if [[ "$#" -gt 0 ]];
then
    CMD=$1
fi

export EXEC_WRAPPER_ENABLE_STATUS_UPDATE_LISTENER=TRUE
export EXEC_WRAPPER_STATUS_UPDATE_INTERVAL_SECONDS=5
python3 exec_wrapper.py $CMD
