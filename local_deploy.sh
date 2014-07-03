#!/bin/bash

##
#
#  Copyright 2014 Netflix, Inc.
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

echo "Deploying Genie to ${CATALINA_HOME}/webapps/ROOT.war"

rm -rf ${CATALINA_HOME}/webapps/ROOT ${CATALINA_HOME}/webapps/ROOT.war
cp ./genie-web/build/libs/genie-*.war ${CATALINA_HOME}/webapps/ROOT.war

#If your tomcat binds to a public address rather than just localhost comment in these lines
#mkdir -p $CATALINA_HOME/webapps/ROOT
#unzip -q $CATALINA_HOME/webapps/ROOT.war -d $CATALINA_HOME/webapps/ROOT/
#GENIE_HOST_NAME=`hostname`
#sed -i "s/localhost/$GENIE_HOST_NAME/g" $CATALINA_HOME/webapps/ROOT/docs/api/index.html
#sed -i "s/localhost/$GENIE_HOST_NAME/g" $CATALINA_HOME/webapps/ROOT/WEB-INF/web.xml

echo "Successfully deployed Genie"
