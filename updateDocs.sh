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

#######################################################################################################
# This script is intended to be run in the master branch to update generated documentation on GitHub. #
#######################################################################################################

###########
# Methods #
###########

cleanup() {
    shutdownTomcat
    rm -rf /tmp/genie
}

checkIfGitRepo() {
    #Make sure current directory is a git repository
    echo "Ensuring it's a git repository"
    git status
    if (( $? !=0 ))
    then
        echo "This is not a git repository. Unable to continue"
        exit 1
    fi
    echo "It's a git repository"
}

checkOnMasterBranch() {
    echo "Making sure this script is being run from master branch"
    GIT_BRANCH_COMMAND="git branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/\1/'"
    BRANCH=`eval ${GIT_BRANCH_COMMAND}`
    if [ "$BRANCH" != "master" ]
    then
        echo "To run this script you must be on the master branch. Currently on $BRANCH"
        exit 1
    fi
    echo "On master branch"
}

checkCatalinaHome() {
    echo "Checking to make sure CATALINA_HOME is set and valid"
    : ${CATALINA_HOME:?"Need to set CATALINA_HOME non-empty"}
    if (( $? != 0 ))
    then
        exit 1
    fi
    if [ ! -f "$CATALINA_HOME/bin/startup.sh" ]
    then
        echo "Not a valid tomcat deployment as $CATALINA_HOME/bin/startup doesn't exist"
        exit 1
    fi
}

updateCode() {
    echo "Updating master to latest"
    git pull
    if (( $? != 0 ))
    then
        echo "Unable to update the master branch. Unable to continue"
        exit 1
    fi
    echo "Successfully updated master"
}

checkBuildGradle() {
    echo "Checking to make sure gradle build file exists"
    if [ ! -f "build.gradle" ]
    then
        echo "gradlew doesn't exist in current branch"
        exit 1
    fi
}

checkGradlew() {
    echo "Checking to make sure gradle wrapper exists"
    if [ ! -f "gradlew" ]
    then
        echo "gradlew doesn't exist in current branch"
        exit 1
    fi
}

buildCode() {
    echo "Building code"
    ./gradlew clean build
    if (( $? != 0 ))
    then
        echo "Unable to build code"
        exit 1
    fi
}

shutdownTomcat() {
    echo "Checking if tomcat is running or not"
    TOMCAT_PID=$(ps axuw | grep ${CATALINA_HOME}/bin | grep -v grep |  awk '{print $2}')
    if [ "$TOMCAT_PID" ]
    then
        echo "Tomcat is running with PID $TOMCAT_PID. Shutting it down"
        ${CATALINA_HOME}/bin/shutdown.sh
        while sleep 1 
        do
            echo "Waiting for Tomcat to shut down"
            ps -p ${TOMCAT_PID} > /dev/null || break;
        done
        echo "Tomcat successfully shut down"
    fi
}

deployGenie() {
    echo "Checking to make sure deploy script exists"
    if [ ! -f "local_deploy.sh" ]
    then
        echo "local_deploy.sh doesn't exist in master branch"
        exit 1
    fi

    echo "Making sure CATALINA_OPTS is right for OSS local deployment"
    export CATALINA_OPTS="-Darchaius.deployment.applicationId=genie -Darchaius.deployment.environment=dev"
    echo "CATALINA_OPTS = $CATALINA_OPTS"

    echo "Deploying the Genie application"
    ./local_deploy.sh
    echo "Successfully deployed the application"
}

checkoutGhPages() {
    pushd /tmp
    if [ -d "/tmp/genie" ]
    then
        rm -rf /tmp/genie
        if (( $? != 0 ))
        then
            "echo genie folder already exists and unable to remove it"
            exit 1
        fi
    fi
    git clone git@github.com:Netflix/genie.git
    if (( $? != 0 ))
    then
        echo "Unable to clone Genie code"
        exit 1
    fi
    echo "Successfully checked out genie"
    cd genie
    git checkout gh-pages
    if (( $? != 0 ))
    then
        echo "Unable to checkout gh-pages branch"
        exit 1
    fi
    popd
}

startTomcat() {
    echo "Starting Tomcat"
    ${CATALINA_HOME}/bin/startup.sh
    until [ $(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080) == "200" ];
    do
        echo "Waiting for Tomcat to start. Sleeping."
        sleep 1
    done
    echo "Tomcat successfully started"
}

updateJavaDocs() {
    #Get rid of the old outdated docs
    rm -rf /tmp/genie/docs/javadoc/*

    mkdir -p /tmp/genie/docs/javadoc/common/
    rsync -r genie-common/build/docs/javadoc/* /tmp/genie/docs/javadoc/common/
    if (( $? != 0 ))
    then
        echo "Unable to update commmon javadocs"
        cleanup
        exit 1
    fi

    mkdir -p /tmp/genie/docs/javadoc/client/
    rsync -r genie-client/build/docs/javadoc/* /tmp/genie/docs/javadoc/client/
    if (( $? != 0 ))
    then
        echo "Unable to update client javadocs"
        cleanup
        exit 1
    fi

    mkdir -p /tmp/genie/docs/javadoc/server/
    rsync -r genie-server/build/docs/javadoc/* /tmp/genie/docs/javadoc/server/
    if (( $? != 0 ))
    then
        echo "Unable to update server javadocs"
        cleanup
        exit 1
    fi

    #Update Git
    pushd /tmp/genie
    git add --all
    git commit -m "Updating Javadocs for latest version of Genie"
    popd
}

updateAPIDocs() {
    shutdownTomcat
    deployGenie
    startTomcat
    #In case it didn't already exist
    if [ ! -d "/tmp/genie/docs/rest" ]
    then
        mkdir -p /tmp/genie/docs/rest
    fi
    curl http://localhost:8080/genie/swagger.json > /tmp/genie/docs/rest/swagger.json
    if (( $? != 0 ))
    then
        echo "Unable to update swagger.json."
        cleanup
        exit 1
    fi

    #Update Git
    pushd /tmp/genie
    git add --all
    git commit -m "Updating Swagger documentation for latest version of Genie"
    popd
    shutdownTomcat
}

updateGit() {
    echo "Pushing updated documenation to GitHub"
    pushd /tmp/genie
    git push
    if (( $? != 0 ))
    then
        echo "Unable to push gh-pages to origin (github). Documentation not updated."
        cleanup
        exit 1
    fi
    popd
    echo "Successfully pushed updated documenation to GitHub"
}

################
# Begin Script #
################

echo "Beginning process of updating documenation for Genie based on code in master branch"

#Get current code and build/deploy to local Tomcat instance
checkIfGitRepo
checkOnMasterBranch
checkCatalinaHome
updateCode
checkBuildGradle
checkGradlew
#TODO: Web-int tests won't work if tomcat running on same port. Need to fix
shutdownTomcat
buildCode

#Get documentation branch from GitHub and clone it in tmp
checkoutGhPages

#Ok should be ready to update documentation
updateJavaDocs
updateAPIDocs
updateGit

cleanup
exit 0
