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

trap "{ archiveToS3; echo 'Job killed'; touch genie.done; exit 211;}" SIGINT SIGTERM

function checkError {
    if [ "$?" -ne 0 ]; then
        archiveToS3
        echo "Job Failed"
        touch genie.done
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

    # Start Non-Generic Code 
    # if HADOOP_HOME is set assume Yarn Job and copy remaining
    # conf files from its conf. 
    if [ -n "$HADOOP_HOME" ]; then
        echo "Copying local Hadoop config files..."
        cp $HADOOP_HOME/conf/* $CURRENT_JOB_CONF_DIR
        checkError 203
        echo "Copied local hadoop files from conf directory"
	echo $'\n'
    fi

    # End Non-Generic Code

    # Setting Cluster, Command and Application Env Variables if specifed
    echo "Setting env variables by sourcing env files for resources"
    setEnvVariables
    checkError 205
    echo "Done setting env variables"
    echo $'\n'

    # Assuming cluster config files HAVE to be specified
    echo "Copying cluster Config files ..."
    copyFiles "$S3_CLUSTER_CONF_FILES" "file://$CURRENT_JOB_CONF_DIR"/
    checkError 206
    echo "Copied cluster config files"
    echo $'\n'
    
    if [ -n "$S3_COMMAND_CONF_FILES" ]; then
        echo "Copying command Config files ..."
        copyFiles "$S3_COMMAND_CONF_FILES" "file://$CURRENT_JOB_CONF_DIR"/
        checkError 207
        echo "Copied command config files"
        echo $'\n'
    fi
    
    if [ -n "$S3_APPLICATION_CONF_FILES" ]; then
        echo "Copying application Config files ..."
        copyFiles "$S3_APPLICATION_CONF_FILES" "file://$CURRENT_JOB_CONF_DIR"/
        checkError 208
        echo "Copied application config files"
        echo $'\n'
    fi

    if [ -n "$S3_APPLICATION_JAR_FILES" ]; then
        echo "Copying application jar files ..."
        copyFiles "$S3_APPLICATION_JAR_FILES" "file://$CURRENT_JOB_JAR_DIR"/
        checkError 209
        echo "Copied application jars files"
    fi

    echo "Copying job dependency files: $JOB_FILE_DEPENDENCIES" 
    # only copy file dependencies if they exist 
    if [ "$CURRENT_JOB_FILE_DEPENDENCIES" != "" ]
    then
        copyFiles "$CURRENT_JOB_FILE_DEPENDENCIES" "file://$CURRENT_JOB_WORKING_DIR"
        checkError 210
        echo "Copied job dependency files"
        echo $'\n'
    fi

    # If core site xml env variable is set, we add all the variables
    # we want to add to yarn job execution there. 
    if [ -n "$CORE_SITE_XML_ARGS" ]; then
        echo "Updating core-site xml args"
        updateCoreSiteXml
        checkError 204
        echo "Updated core-site xml args"
        echo $'\n'
    fi
    
    mkdir tmp
    echo "Executing CMD: $CMD $@"
    $CMD "$@" 1>$CURRENT_JOB_WORKING_DIR/stdout.log 2>$CURRENT_JOB_WORKING_DIR/stderr.log
    checkError 213
}

function updateCoreSiteXml {
    
    # set the following variables
    # genie.job.id, netflix.environment, lipstick.uuid.prop.name from CORE_SITE_XML_ARGS
    # set dataoven.gateway.type genie
    # dataoven.job.id
    kvarr=$(echo $CORE_SITE_XML_ARGS | tr ";" "\n")
    for item in $kvarr
    do
        key=$(echo $item | awk -F'=' '{print $1}')
        value=$(echo $item | awk -F'=' '{print $2}')
        appendKeyValueToCoreSite $key $value
    done
    appendKeyValueToCoreSite "genie.version" "2" 
return 0
}

function appendKeyValueToCoreSite {

    echo "Appending $1/$2 to core-site.xml as key/value pair."
    KEY=$1
    VALUE=$2
    SEARCH_PATTERN="</configuration>"
    REPLACE_PATTERN="<property><name>$KEY</name><value>$VALUE</value></property>\n&"
    sed -i "s|$SEARCH_PATTERN|$REPLACE_PATTERN|" $CURRENT_JOB_CONF_DIR/core-site.xml
}

function setEnvVariables {
    
    if [ -n "$APPLICATION_ENV_FILE" ]
    then
        echo "Copy down and Source Application Env File"
        copyFiles "$APPLICATION_ENV_FILE" "file://$CURRENT_JOB_CONF_DIR"/
        APP_FILENAME=`basename $APPLICATION_ENV_FILE`
        echo "App Env Filename: $APP_FILENAME"
        source "$CURRENT_JOB_CONF_DIR/$APP_FILENAME" 
        echo "Application Name = $APPNAME"
    fi

    if [ -n "$COMMAND_ENV_FILE" ]
    then
        echo "Copy down and Source Command Env File"
        copyFiles "$COMMAND_ENV_FILE" "file://$CURRENT_JOB_CONF_DIR"/
        COMMAND_FILENAME=`basename $COMMAND_ENV_FILE`
        echo "Command Env Filename: $COMMAND_FILENAME"
        source "$CURRENT_JOB_CONF_DIR/$COMMAND_FILENAME" 
        echo "Command Name=$CMDNAME"
    fi

    if [ -n "$JOB_ENV_FILE" ]
    then
        echo "Copy down and Source Job File"
        copyFiles "$JOB_ENV_FILE" "file://$CURRENT_JOB_CONF_DIR"/
        JOB_FILENAME=`basename $JOB_ENV_FILE`
        echo "Job Env Filename: $JOB_FILENAME"
        source "$CURRENT_JOB_CONF_DIR/$JOB_FILENAME" 
        echo "Job Name = $JOBNAME"
    fi
    return 0
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

echo "Job Execution Env Variables"
echo "****************************************************************"
env
echo "****************************************************************"
echo $'\n'

echo "Creating job jar dir: $CURRENT_JOB_JAR_DIR"
mkdir -p $CURRENT_JOB_JAR_DIR 
checkError 201
echo "Job Jar dir created"
echo $'\n'

echo "Creating job conf dir: $CURRENT_JOB_CONF_DIR"
mkdir -p $CURRENT_JOB_CONF_DIR 
checkError 202
echo "Job conf directory created"
echo $'\n'

# Uncomment the following if you want Genie to create users if they don't exist already
#echo "Create user.group $USER_NAME.$GROUP_NAME, if it doesn't exist already"
#sudo groupadd $GROUP_NAME
#sudo useradd $USER_NAME -g $GROUP_NAME
#echo "User and Group created"
#echo $'\n'

executeCommand "$@"
archiveToS3

echo "Done"
touch genie.done
exit 0
