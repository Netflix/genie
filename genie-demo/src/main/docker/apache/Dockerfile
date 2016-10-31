from httpd:2.4
MAINTAINER NetflixOSS <netflixoss@netflix.com>
COPY ./files/ /usr/local/apache2/htdocs/
ADD http://apache.cs.utah.edu/hadoop/common/hadoop-2.7.1/hadoop-2.7.1.tar.gz /usr/local/apache2/htdocs/applications/hadoop/2.7.1/
ADD http://d3kbcqa49mib13.cloudfront.net/spark-2.0.1-bin-hadoop2.7.tgz /usr/local/apache2/htdocs/applications/spark/2.0.1/
#ADD http://apache.cs.utah.edu/pig/pig-0.16.0/pig-0.16.0.tar.gz /usr/local/apache2/htdocs/applications/pig/0.16.0/
#ADD http://apache.cs.utah.edu/hive/hive-2.1.0/apache-hive-2.1.0-bin.tar.gz /usr/local/apache2/htdocs/applications/hive/2.1.0/
RUN chmod -R +r /usr/local/apache2/htdocs/
