#!/bin/bash

set -x
set -e

# Install Tomcat
cd $HOME; wget http://mirror.sdunix.com/apache/tomcat/tomcat-6/v6.0.37/bin/apache-tomcat-6.0.37.tar.gz
tar zxvf apache-tomcat-6.0.37.tar.gz

# Change port to 7001 to work out of the box
sed -i -e "s#8080#7001#g"  /home/hadoop/apache-tomcat-6.0.37/conf/server.xml
    
# Update /home/hadoop/apache-tomcat-6.0.37/conf/web.xml to enable directory browsing

# Set up Genie specific properties
export CATALINA_HOME=/home/hadoop/apache-tomcat-6.0.37
export CATALINA_OPTS="-Darchaius.deployment.applicationId=genie -Dnetflix.datacenter=cloud"

# Set up directories needed
mkdir -p /mnt/tomcat/genie-jobs;
ln -fs /mnt/tomcat/genie-jobs $CATALINA_HOME/webapps
mkdir -p /home/hadoop/.versions/pig-0.11.1/conf; touch /home/hadoop/.versions/pig-0.11.1/conf/pig.properties

# Set up genie
# can clone from master when the topic branch is merged
git clone -b issues/9 https://github.com/Netflix/genie.git
cd $HOME/genie; ./gradlew clean build -x test
cd $HOME/genie; ./local_deploy.sh

# Start Tomcat
cd $CATALINA_HOME/logs; $CATALINA_HOME/bin/startup.sh;
