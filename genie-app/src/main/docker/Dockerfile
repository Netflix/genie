from openjdk:8
MAINTAINER NetflixOSS <netflixoss@netflix.com>
ARG JAR_NAME
ARG VERSION
EXPOSE 8080
VOLUME /tmp
ADD ${JAR_NAME}-${VERSION}.jar /usr/local/bin/genie.jar
RUN sh -c "touch /usr/local/bin/genie.jar"
ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/usr/local/bin/genie.jar"]
