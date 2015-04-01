#!/bin/bash

##
#
#  Copyright 2015 Netflix, Inc.
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
        echo "$(date +"%F %T.%3N") Job Failed"
        touch genie.done
        exit $1
    fi
}

function copyFiles {
    # Replace the comma's (expected with internal fs.py script) with spaces (expected by Hadoop fs command)
    SOURCE=$(echo "$1" | sed 's/,/ /g')
    DESTINATION=$2

    # number of retries for s3cp
    NUM_RETRIES=5
    
    # run hadoop fs -cp via timeout, so it doesn't hang indefinitely
    TIMEOUT="${XS_SYSTEM_HOME}/timeout3 -t ${CP_TIMEOUT}"
    
    # copy over the files to/from S3 - retry $NUM_RETRIES times
    i=0
    retVal=0
    echo "$(date +"%F %T.%3N") Copying files ${SOURCE} to ${DESTINATION}"
    while true
    do
        ${TIMEOUT} ${COPY_COMMAND} ${SOURCE} ${DESTINATION}/
        retVal="$?"
        if [ "${retVal}" -eq 0 ]; then
            break
        else
            echo "$(date +"%F %T.%3N") Will retry in 5 seconds to ensure that this is not a transient error"
            sleep 5
            i=$((${i}+1))
        fi
	
        # exit with error if done retrying
        if [ "${i}" -eq "${NUM_RETRIES}" ]; then
            echo "$(date +"%F %T.%3N") Failed to copy files from ${SOURCE} to ${DESTINATION}"
            break
        fi  
    done

    # return 0 or error code from s3cp
    return ${retVal}
}

function setupCluster {
    if [ -n "${S3_CLUSTER_CONF_FILES}" ]; then
        echo "$(date +"%F %T.%3N") Copying cluster Config files ..."
        copyFiles "${S3_CLUSTER_CONF_FILES}" "file://${CURRENT_JOB_CONF_DIR}"/
        checkError 206
        echo "$(date +"%F %T.%3N") Copied cluster config files"
        echo $'\n'
    fi

    return 0
}

function setupApplication {
    if [ -n "${S3_APPLICATION_JAR_FILES}" ]; then
        echo "$(date +"%F %T.%3N") Copying application jar files ..."
        copyFiles "${S3_APPLICATION_JAR_FILES}" "file://${CURRENT_JOB_JAR_DIR}"/
        checkError 209
        echo "$(date +"%F %T.%3N") Copied application jars files"
    fi

    if [ -n "${S3_APPLICATION_CONF_FILES}" ]; then
        echo "$(date +"%F %T.%3N") Copying application Config files ..."
        copyFiles "${S3_APPLICATION_CONF_FILES}" "file://${CURRENT_JOB_CONF_DIR}"/
        checkError 208
        echo "$(date +"%F %T.%3N") Copied application config files"
        echo $'\n'
    fi

    if [ -n "${APPLICATION_ENV_FILE}" ]
    then
        echo "$(date +"%F %T.%3N") Copy down and Source Application Env File"
        copyFiles "${APPLICATION_ENV_FILE}" "file://${CURRENT_JOB_CONF_DIR}"/
        checkError 205
        APP_FILENAME=`basename ${APPLICATION_ENV_FILE}`
        echo "$(date +"%F %T.%3N") App Env Filename: ${APP_FILENAME}"
        source "${CURRENT_JOB_CONF_DIR}/${APP_FILENAME}"
        checkError 205
        echo "$(date +"%F %T.%3N") Application Name = ${APPNAME}"
    fi

    return 0
}

function setupCommand {
    if [ -n "${S3_COMMAND_CONF_FILES}" ]; then
        echo "$(date +"%F %T.%3N") Copying command Config files ..."
        copyFiles "${S3_COMMAND_CONF_FILES}" "file://${CURRENT_JOB_CONF_DIR}"/
        checkError 207
        echo "$(date +"%F %T.%3N") Copied command config files"
        echo $'\n'
    fi

    if [ -n "${COMMAND_ENV_FILE}" ]
    then
        echo "$(date +"%F %T.%3N") Copy down and Source Command Env File"
        copyFiles "${COMMAND_ENV_FILE}" "file://${CURRENT_JOB_CONF_DIR}"/
        checkError 205
        COMMAND_FILENAME=`basename ${COMMAND_ENV_FILE}`
        echo "$(date +"%F %T.%3N") Command Env Filename: ${COMMAND_FILENAME}"
        source "${CURRENT_JOB_CONF_DIR}/${COMMAND_FILENAME}"
        checkError 205
        echo "$(date +"%F %T.%3N") Command Name=${CMDNAME}"
    fi

    return 0
}

function setupJob {
    if [ -n "${JOB_ENV_FILE}" ]
    then
        echo "$(date +"%F %T.%3N") Copy down and Source Job File"
        copyFiles "${JOB_ENV_FILE}" "file://${CURRENT_JOB_CONF_DIR}"/
        checkError 205
        JOB_FILENAME=`basename ${JOB_ENV_FILE}`
        echo "$(date +"%F %T.%3N") Job Env Filename: ${JOB_FILENAME}"
        source "${CURRENT_JOB_CONF_DIR}/${JOB_FILENAME}"
        checkError 205
        echo "$(date +"%F %T.%3N") Job Name = ${JOBNAME}"
    fi

    echo "$(date +"%F %T.%3N") Copying job dependency files: ${JOB_FILE_DEPENDENCIES}"
    # only copy file dependencies if they exist
    if [ "${CURRENT_JOB_FILE_DEPENDENCIES}" != "" ]
    then
        copyFiles "${CURRENT_JOB_FILE_DEPENDENCIES}" "file://${CURRENT_JOB_WORKING_DIR}"
        checkError 210
        echo "$(date +"%F %T.%3N") Copied job dependency files"
        echo $'\n'
    fi

    return 0
}

function updateCoreSiteXml {
    # set the following variables
    # genie.job.id, netflix.environment, lipstick.uuid.prop.name from CORE_SITE_XML_ARGS
    # set dataoven.gateway.type genie
    # dataoven.job.id
    kvarr=$(echo ${CORE_SITE_XML_ARGS} | tr ";" "\n")
    for item in ${kvarr}
    do
        key=$(echo ${item} | awk -F'=' '{print $1}')
        value=$(echo ${item} | awk -F'=' '{print $2}')
        appendKeyValueToCoreSite ${key} ${value}
    done
    appendKeyValueToCoreSite "genie.version" "2"
    return 0
}

function appendKeyValueToCoreSite {
    echo "$(date +"%F %T.%3N") Appending ${1}/${2} to core-site.xml as key/value pair."
    KEY=$1
    VALUE=$2
    SEARCH_PATTERN="</configuration>"
    REPLACE_PATTERN="<property><name>${KEY}</name><value>${VALUE}</value></property>\n&"
    sed -i "s|${SEARCH_PATTERN}|${REPLACE_PATTERN}|" ${CURRENT_JOB_CONF_DIR}/core-site.xml
}

function archiveToS3 {
    # if the s3 archive location is not set, return immediately
    if [ "${S3_ARCHIVE_LOCATION}" == "" ]
    then
        echo "$(date +"%F %T.%3N") Not archiving files in working directory"
        return
    fi

    S3_PREFIX=${S3_ARCHIVE_LOCATION}/`basename ${PWD}`
    echo "$(date +"%F %T.%3N") Archiving all files in working directory to ${S3_PREFIX}"

    # find the files to copy
    SOURCE=`find . -maxdepth 2 -type f | grep -v conf`
    TARBALL="logs.tar.gz"
    tar -czf ${TARBALL} ${SOURCE}

    # if it fails to create tarball, just move on
    if [ "$?" -ne 0 ]; then
        echo "$(date +"%F %T.%3N") Failed to archive logs to tarball - but moving on"
        return
    fi

    # create a directory first
    ${MKDIR_COMMAND} ${S3_PREFIX}

    # copy over the logs to S3
    copyFiles "file://${PWD}/${TARBALL}" "${S3_PREFIX}/${TARBALL}"

    # if it fails, just move on
    if [ "$?" -ne 0 ]; then
        echo "$(date +"%F %T.%3N") Failed to archive logs to S3 - but moving on"
    fi
}

function executeCommand {
    # Start Non-Generic Code 
    # if HADOOP_HOME is set assume Yarn Job and copy remaining
    # conf files from its conf. 
    if [ -n "${HADOOP_HOME}" ]; then
        echo "$(date +"%F %T.%3N") Copying local Hadoop config files..."
        cp ${HADOOP_HOME}/conf/* ${CURRENT_JOB_CONF_DIR}
        checkError 203
        echo "$(date +"%F %T.%3N") Copied local hadoop files from conf directory"
	echo $'\n'
    fi

    # End Non-Generic Code

    # Setup all necessary parts
    setupApplication
    setupCommand
    setupCluster
    setupJob

    # If core site xml env variable is set, we add all the variables
    # we want to add to yarn job execution there. 
    if [ -n "${CORE_SITE_XML_ARGS}" ]; then
        echo "$(date +"%F %T.%3N") Updating core-site xml args"
        updateCoreSiteXml
        checkError 204
        echo "$(date +"%F %T.%3N") Updated core-site xml args"
        echo $'\n'
    fi
    
    mkdir tmp
    echo "$(date +"%F %T.%3N") Executing CMD: ${CMD} $@"
    ${CMD} "$@" 1>${CURRENT_JOB_WORKING_DIR}/stdout.log 2>${CURRENT_JOB_WORKING_DIR}/stderr.log
    checkError 213
}

ARGS=$#
CMD=$1
shift
CMDLINE="$@"

CMDLOG=${CURRENT_JOB_WORKING_DIR}/cmd.log
exec > ${CMDLOG} 2>&1

echo "$(date +"%F %T.%3N") Job Execution Env Variables"
echo "$(date +"%F %T.%3N") ****************************************************************"
env
echo "$(date +"%F %T.%3N") ****************************************************************"
echo $'\n'

echo "$(date +"%F %T.%3N") Creating job jar dir: ${CURRENT_JOB_JAR_DIR}"
mkdir -p ${CURRENT_JOB_JAR_DIR}
checkError 201
echo "$(date +"%F %T.%3N") Job Jar dir created"
echo $'\n'

echo "$(date +"%F %T.%3N") Creating job conf dir: ${CURRENT_JOB_CONF_DIR}"
mkdir -p ${CURRENT_JOB_CONF_DIR}
checkError 202
echo "$(date +"%F %T.%3N") Job conf directory created"
echo $'\n'

# Uncomment the following if you want Genie to create users if they don't exist already
echo "$(date +"%F %T.%3N") Create user.group ${USER_NAME}.${GROUP_NAME}, if it doesn't exist already"
sudo groupadd ${GROUP_NAME}
sudo useradd ${USER_NAME} -g ${GROUP_NAME}
echo "$(date +"%F %T.%3N") User and Group created"
echo $'\n'

executeCommand "$@"
archiveToS3

echo "$(date +"%F %T.%3N") Done"
touch genie.done
exit 0
