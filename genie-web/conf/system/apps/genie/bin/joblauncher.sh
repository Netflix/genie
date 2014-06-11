#!/bin/bash

##
#
#  Copyright 2013 Netflix, Inc.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
#
##

trap "{ archiveToS3; echo 'Job killed'; exit 211;}" SIGINT SIGTERM

function checkError {
    if [ "$?" -ne 0 ]; then
        archiveToS3
        exit $1
    fi
}

function copyFiles {
    SOURCE=$1
    DESTINATION=$2

    # use hadoop for s3 copying
    S3CP="$HADOOP_HOME/bin/hadoop fs $HADOOP_S3CP_OPTS -cp $FORCE_COPY_FLAG "
    
    # number of retries for s3cp
    NUM_RETRIES=5

    # convert CSV to be space separated
    SOURCE=`echo $SOURCE | sed -e 's/,/ /g'`
    
    # run hadoop fs -cp via timeout, so it doesn't hang indefinitely
    TIMEOUT="$XS_SYSTEM_HOME/timeout3 -t $HADOOP_S3CP_TIMEOUT"
    
    # copy over the files to/from S3 - retry $NUM_RETRIES times
    i=0
    retVal=0
    echo "Copying files $SOURCE to $DESTINATION"
    while true
    do
        $TIMEOUT $S3CP ${SOURCE} ${DESTINATION}/
        retVal="$?"
        if [ "$retVal" -eq 0 ]; then
            break
        else
            echo "Will retry in 5 seconds to ensure that this is not a transient error"
            sleep 5
            i=$(($i+1))
        fi
	
        # exit with error if done retrying
        if [ "$i" -eq "$NUM_RETRIES" ]; then
            echo "Failed to copy files from $SOURCE to $DESTINATION"
            break
        fi  
    done

    # return 0 or error code from s3cp
    return $retVal
}

function executeHadoop {
    echo "Copying Hadoop config files..." 
    cp $HADOOP_HOME/conf/* $CURRENT_JOB_CONF_DIR
    checkError 204
    copyFiles "$S3_HADOOP_CONF_FILES" "file://$CURRENT_JOB_CONF_DIR"
    checkError 204

    echo "Executing CMD: $CMD $@"
    HADOOP_CONF_DIR=$CURRENT_JOB_CONF_DIR \
	$HADOOP_HOME/bin/$CMD "$@" 1>$CURRENT_JOB_WORKING_DIR/stdout.log 2>$CURRENT_JOB_WORKING_DIR/stderr.log
    checkError 206
}


function executeHive {
    echo "Copying Hadoop config files..."
    cp $HADOOP_HOME/conf/* $CURRENT_JOB_CONF_DIR
    checkError 204
    copyFiles "$S3_HADOOP_CONF_FILES" "file://$CURRENT_JOB_CONF_DIR"/
    checkError 204

    echo "Copying Hive config files..."
    cp $HIVE_HOME/conf/* $CURRENT_JOB_CONF_DIR/
    checkError 205
    copyFiles "$S3_HIVE_CONF_FILES" "file://$CURRENT_JOB_CONF_DIR"/
    checkError 205

    # create a tmp dir for java.io.tmpdir
    mkdir tmp
    
    echo "Executing CMD: $CMD $@"
    HADOOP_CONF_DIR=$CURRENT_JOB_CONF_DIR HIVE_CONF_DIR=$CURRENT_JOB_CONF_DIR \
	$HIVE_HOME/bin/$CMD "$@" -hiveconf java.io.tmpdir=$PWD/tmp -hiveconf hive.downloaded.resources.dir=downloads \
    	-hiveconf mapred.local.dir=tmp -hiveconf hive.log.dir=hivelogs -hiveconf hive.log.file=hive.log \
    	-hiveconf dataoven.gateway.type=genie -hiveconf dataoven.job.id=hive_`date +%s`_`basename $PWD` \
    	-hiveconf hive.querylog.location=hivelogs 1>$CURRENT_JOB_WORKING_DIR/stdout.log 2>$CURRENT_JOB_WORKING_DIR/stderr.log
    checkError 206
}

function executePig {
    echo "Copying Hadoop config files..."
    cp $HADOOP_HOME/conf/* $CURRENT_JOB_CONF_DIR
    checkError 204
    copyFiles "$S3_HADOOP_CONF_FILES" "file://$CURRENT_JOB_CONF_DIR"/
    checkError 204

    echo "Copying Pig config files..."
    copyFiles "$S3_PIG_CONF_FILES" "file://$CURRENT_JOB_CONF_DIR"/
    checkError 209

    # create and set java.io.tmpdir for pig
    mkdir tmp
    export HADOOP_OPTS="-Djava.io.tmpdir=$PWD/tmp"
    
    export PIG_LOG_DIR=$CURRENT_JOB_WORKING_DIR/piglogs

    echo "Executing CMD: $CMD $@"
    HADOOP_CONF_DIR=$CURRENT_JOB_CONF_DIR PIG_CONF_DIR=$CURRENT_JOB_CONF_DIR \
	$PIG_HOME/bin/$CMD -D dataoven.gateway.type=genie -D dataoven.job.id=pig_`date +%s`_`basename $PWD` \
    	"$@" 1>$CURRENT_JOB_WORKING_DIR/stdout.log 2>$CURRENT_JOB_WORKING_DIR/stderr.log
    checkError 206
}

function executeCommand {
    if [ "$CMD" = "hadoop" ]; then
        executeHadoop "$@"
        return 0   
    fi
    if [ "$CMD" = "hive" ]; then
        executeHive "$@"
        return 0   
    fi
    if [ "$CMD" = "pig" ]; then
        executePig "$@"
        return 0   
    fi
    echo "Wrong CMD: $CMD" 
    exit 206
}

function archiveToS3 {
    # if the s3 archive location is not set, return immediately
    if [ "$S3_ARCHIVE_LOCATION" == "" ]
    then
        echo "Not archiving files in working directory"
        return
    fi

    S3_PREFIX=$S3_ARCHIVE_LOCATION/`basename $PWD`
    echo "Archiving all files in working directory to $S3_PREFIX"
    
    # find the files to copy
    SOURCE=`find . -maxdepth 2 -type f | grep -v conf`
    TARBALL="logs.tar.gz"
    tar -czf $TARBALL $SOURCE 
    
    # if it fails to create tarball, just move on
    if [ "$?" -ne 0 ]; then
        echo "Failed to archive logs to tarball - but moving on"
        return
    fi
        
    # create a directory first
    $HADOOP_HOME/bin/hadoop fs $HADOOP_S3CP_OPTS -mkdir $S3_PREFIX

    # copy over the logs to S3
    copyFiles "file://$PWD/$TARBALL" "$S3_PREFIX"
    
    # if it fails, just move on
    if [ "$?" -ne 0 ]; then
        echo "Failed to archive logs to S3 - but moving on"
    fi
}

ARGS=$#
CMD=$1
shift
CMDLINE="$@"

CMDLOG=$CURRENT_JOB_WORKING_DIR/cmd.log
exec > $CMDLOG 2>&1

echo "Job Execution Parameters" 
echo "ARGS = $ARGS" 
echo "CMD = $CMD" 
echo "CMDLINE = $CMDLINE" 
echo "CURRENT_JOB_FILE_DEPENDENCIES = $CURRENT_JOB_FILE_DEPENDENCIES" 
echo "S3_HADOOP_CONF_FILES = $S3_HADOOP_CONF_FILES" 
echo "S3_HIVE_CONF_FILES = $S3_HIVE_CONF_FILES" 
echo "S3_PIG_CONF_FILES = $S3_PIG_CONF_FILES" 
echo "CURRENT_JOB_WORKING_DIR = $CURRENT_JOB_WORKING_DIR" 
echo "CURRENT_JOB_CONF_DIR = $CURRENT_JOB_CONF_DIR" 
echo "S3_ARCHIVE_LOCATION = $S3_ARCHIVE_LOCATION"
echo "HADOOP_USER_NAME = $HADOOP_USER_NAME"
echo "HADOOP_GROUP_NAME = $HADOOP_GROUP_NAME"
echo "HADOOP_HOME = $HADOOP_HOME"
echo "HIVE_HOME = $HIVE_HOME"
echo "PIG_HOME = $PIG_HOME"
echo "HADOOP_S3CP_TIMEOUT = $HADOOP_S3CP_TIMEOUT"
echo "FORCE_COPY_FLAG = $FORCE_COPY_FLAG"

echo "Creating job conf dir: $CURRENT_JOB_CONF_DIR"
mkdir -p $CURRENT_JOB_CONF_DIR 
checkError 202

echo "Copying job dependency files: $JOB_FILE_DEPENDENCIES" 
# only copy file dependencies if they exist 
if [ "$CURRENT_JOB_FILE_DEPENDENCIES" != "" ]
then
    copyFiles "$CURRENT_JOB_FILE_DEPENDENCIES" "file://$CURRENT_JOB_WORKING_DIR"
    checkError 203
fi

# Uncomment the following if you want Genie to create users if they don't exist already
# echo "Create user.group $HADOOP_USER_NAME.$HADOOP_GROUP_NAME, if it doesn't exist already"
# sudo groupadd $HADOOP_GROUP_NAME
# sudo useradd $HADOOP_USER_NAME -g $HADOOP_GROUP_NAME

executeCommand "$@"

archiveToS3

echo "Done"
exit 0

