from openjdk:11-jre-slim
MAINTAINER NetflixOSS <netflixoss@netflix.com>
EXPOSE 8080
VOLUME /tmp
ARG SERVER_JAR
ARG AGENT_JAR
RUN apt-get update && \
  apt-get install -y --no-install-recommends procps && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/
COPY ${SERVER_JAR} /usr/local/bin/genie-server.jar
COPY ${AGENT_JAR} /usr/local/bin/genie-agent.jar
ENTRYPOINT ["java", \
            "-Djava.security.egd=file:/dev/./urandom", \
            "-Dgenie.agent.launcher.local.agent-jar-path=/usr/local/bin/genie-agent.jar", \
            "-Dgenie.jobs.agent-execution.agent-probability=1.0", \
            "-jar", \
            "/usr/local/bin/genie-server.jar" \
            ]
