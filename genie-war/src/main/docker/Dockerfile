from tomcat:8.0-jre8
MAINTAINER NetflixOSS <netflixoss@netflix.com>
VOLUME /tmp
RUN ["rm", "-rf", "/usr/local/tomcat/webapps/ROOT", "/usr/local/tomcat/work"]
ARG WAR_NAME
ARG VERSION
ADD ${WAR_NAME}-${VERSION}.war /usr/local/tomcat/webapps/ROOT.war
