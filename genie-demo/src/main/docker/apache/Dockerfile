from httpd:2.4-alpine
MAINTAINER NetflixOSS <netflixoss@netflix.com>
# TODO: Possiblly we could make these dependency locations of app that point directly to apache. Would make initial
#       Startup slow for first job that executes though...
# NOTE: Doing it using wget instead of ADD because ADD won't use cache layer properly unless BuildKit is enabled
#       and Travis still is on too old a version of docker to enable BuildKit
RUN mkdir -p /usr/local/apache2/htdocs/applications/hadoop/2.7.1/ && \
    wget -P /usr/local/apache2/htdocs/applications/hadoop/2.7.1/ https://archive.apache.org/dist/hadoop/common/hadoop-2.7.1/hadoop-2.7.1.tar.gz && \
    mkdir -p /usr/local/apache2/htdocs/applications/trino/374/ && \
    wget -P /usr/local/apache2/htdocs/applications/trino/374/ https://repo1.maven.org/maven2/io/trino/trino-cli/374/trino-cli-374-executable.jar && \
    mkdir -p /usr/local/apache2/htdocs/applications/spark/2.0.1/ && \
    wget -P /usr/local/apache2/htdocs/applications/spark/2.0.1/ https://archive.apache.org/dist/spark/spark-2.0.1/spark-2.0.1-bin-hadoop2.7.tgz && \
    mkdir -p /usr/local/apache2/htdocs/applications/spark/2.1.3/ && \
    wget -P /usr/local/apache2/htdocs/applications/spark/2.1.3/ https://archive.apache.org/dist/spark/spark-2.1.3/spark-2.1.3-bin-hadoop2.7.tgz
# NOTE: Any version after 2.1.3 is compiled to run against Java 8 it seems. The Hadoop container still runs java 7
#       So as of now unless we find a new Hadoop container none of thse work and throw the old 52.0 byte code error
#    wget -P /usr/local/apache2/htdocs/applications/spark/2.2.3/ https://archive.apache.org/dist/spark/spark-2.2.3/spark-2.2.3-bin-hadoop2.7.tgz && \
#    wget -P /usr/local/apache2/htdocs/applications/spark/2.3.4/ https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz && \
#    wget -P /usr/local/apache2/htdocs/applications/spark/2.4.6/ https://archive.apache.org/dist/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz && \
#    wget -P /usr/local/apache2/htdocs/applications/spark/3.0.0/ https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz
COPY ./files/ /usr/local/apache2/htdocs/
RUN chmod -R +r /usr/local/apache2/htdocs/
