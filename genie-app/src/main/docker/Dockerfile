from openjdk:8-jre
MAINTAINER NetflixOSS <netflixoss@netflix.com>
EXPOSE 8080
VOLUME /tmp
ARG JAR_NAME
ARG VERSION
ADD ${JAR_NAME}-${VERSION}.jar /usr/local/bin/genie.jar
RUN sh -c "touch /usr/local/bin/genie.jar"
ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/usr/local/bin/genie.jar"]
