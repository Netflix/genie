from openjdk:8-jre
MAINTAINER NetflixOSS <netflixoss@netflix.com>
EXPOSE 8080
VOLUME /tmp
RUN apt-get update && apt-get install -y --no-install-recommends procps
ARG JAR_NAME
ARG VERSION
ADD ${JAR_NAME}-${VERSION}.jar /usr/local/bin/genie.jar
RUN sh -c "touch /usr/local/bin/genie.jar"
ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/usr/local/bin/genie.jar"]
