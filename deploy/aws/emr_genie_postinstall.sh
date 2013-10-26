#!/bin/bash

# On Master

# ensure that HDFS is writable by all users (e.g. genietest)
hadoop fs -chmod -R 777 /

# Register EMR cluster
cd $HOME/genie/genie-web/src/test/python/utils
export SERVICE_BASE_URL=http://localhost:7001
python populateEMRConfigs.py

# Test some jobs

cd $HOME/genie/genie-web/src/test/python/jobs
export GENIE_TEST_PREFIX=file:///home/hadoop
python hadoopFSTest.py

python hiveJobTestWithAttachments.py

python pigJobTestWithAttachments.py
