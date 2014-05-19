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

    # number of retries for s3cp
    NUM_RETRIES=5

    # convert CSV to be space separated
    SOURCE=`echo $SOURCE | sed -e 's/,/ /g'`
    
    # run hadoop fs -cp via timeout, so it doesn't hang indefinitely
    TIMEOUT="$XS_SYSTEM_HOME/timeout3 -t $CP_TIMEOUT"
    
    # copy over the files to/from S3 - retry $NUM_RETRIES times
    i=0
    retVal=0
    echo "Copying files $SOURCE to $DESTINATION"
    while true
    do
        $TIMEOUT $COPY_COMMAND ${SOURCE} ${DESTINATION}/
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

function executeCommand {

    # Assuming cluster config files HAVE to be specified
    echo "Copying cluster Config files ..."
    copyFiles "S3_CLUSTER_CONF_FILES" "file://$CURRENT_JOB_CONF_DIR"/
    checkError 204
    
    # Start Non-Generic Code 
    # if HADOOP_HOME is set assume Yarn Job and copy remaining
    # conf files from its conf. Maybe be can put everything on
    # s3
    if [ -n "$HADOOP_HOME" ]; then
        echo "Copying Hadoop config files..."
        cp $HADOOP_HOME/conf/* $CURRENT_JOB_CONF_DIR
        checkError 204
    fi
   
    # If core site xml env variable is set, we add all the variables
    # we want to add to yarn job execution there. 
    if [ -n "$CORE_SITE_XML_ARGS" ]; then
        updateCoreSiteXml
    fi
    # End Non-Geneic Code

    if [ -n "$S3_COMMAND_CONF_FILES" ]; then
        echo "Copying command Config files ..."
        copyFiles "S3_COMMAND_CONF_FILES" "file://$CURRENT_JOB_CONF_DIR"/
        checkError 204
    fi
    
    if [ -n "$S3_APPLICATION_CONF_FILES" ]; then
        echo "Copying application Config files ..."
        copyFiles "S3_APPLICATION_CONF_FILES" "file://$CURRENT_JOB_CONF_DIR"/
        checkError 204
    fi

    if [ -n "$S3_APPLICATION_JAR_FILES" ]; then
        echo "Copying application jar files ..."
        copyFiles "S3_APPLICATION_JAR_FILES" "file://$CURRENT_JOB_JAR_DIR"/
        checkError 204
    fi
    
    mkdir tmp
    echo "Executing CMD: $CMD $@"
    $CMD "$@" 1>$CURRENT_JOB_WORKING_DIR/stdout.log 2>$CURRENT_JOB_WORKING_DIR/stderr.log
    checkError 206
}

function updateCoreSiteXml {
    
    echo "updating core-site xml"
    # set the following variables
    # genie.job.id, netflix.environment, lipstick.uuid.prop.name from CORE_SITE_XML_ARGS
    # set dataoven.gateway.type genie
    # dataoven.job.id
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
    $MKDIR_COMMAND $S3_PREFIX

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
echo "S3_CLUSTER_CONF_FILES = $S3_CLUSTER_CONF_FILES" 
echo "S3_COMMAND_CONF_FILES = $S3_COMMAND_CONF_FILES" 
echo "S3_APPLICATION_CONF_FILES = $S3_APPLICATION_CONF_FILES" 
echo "S3_APPLICATION_JAR_FILES = $S3_APPLICATION_JAR_FILES" 
echo "CURRENT_JOB_WORKING_DIR = $CURRENT_JOB_WORKING_DIR" 
echo "CURRENT_JOB_CONF_DIR = $CURRENT_JOB_CONF_DIR" 
echo "CURRENT_JOB_JAR_DIR = $CURRENT_JOB_JAR_DIR" 
echo "S3_ARCHIVE_LOCATION = $S3_ARCHIVE_LOCATION"
echo "HADOOP_USER_NAME = $HADOOP_USER_NAME"
echo "HADOOP_GROUP_NAME = $HADOOP_GROUP_NAME"
echo "HADOOP_HOME = $HADOOP_HOME"
echo "CP_TIMEOUT = $CP_TIMEOUT"
echo "COPY_COMMAND = $COPY_COMMAND"
echo "MKDIR_COMMAND = $MKDIR_COMMAND"

echo "All Env Variables are"
echo "****************************************************************"
env
echo "****************************************************************"

echo "Creating job conf dir: $CURRENT_JOB_CONF_DIR"
mkdir -p $CURRENT_JOB_CONF_DIR 
checkError 202

echo "Creating job jar dir: $CURRENT_JOB_JAR_DIR"
mkdir -p $CURRENT_JOB_JAR_DIR 
checkError 202

echo "Copying job dependency files: $JOB_FILE_DEPENDENCIES" 
# only copy file dependencies if they exist 
if [ "$CURRENT_JOB_FILE_DEPENDENCIES" != "" ]
then
    copyFiles "$CURRENT_JOB_FILE_DEPENDENCIES" "file://$CURRENT_JOB_WORKING_DIR"
    checkError 203
fi

# Uncomment the following if you want Genie to create users if they don't exist already
# echo "Create user.group $USER_NAME.$GROUP_NAME, if it doesn't exist already"
# sudo groupadd $GROUP_NAME
# sudo useradd $USER_NAME -g $GROUP_NAME

executeCommand "$@"

archiveToS3

echo "Done"
exit 0
