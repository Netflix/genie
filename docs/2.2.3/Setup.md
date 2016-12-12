---
layout: page
title: Setup
teaser: How to install and configure Genie 2.2.3
header: no
permalink: /releases/2.2.3/setup.html
sidebar: left
---

## Introduction

These instructions walk through setting up Genie from scratch. If you just want
to quickly evaluate Genie it's recommended you check out the
[Genie Docker image](https://github.com/Netflix-Skunkworks/zerotodocker/wiki/Genie)
. Also seeing the Dockerfile for the release will help understand the
instructions contained in here further.

## Assumptions

- These instructions are for the current Genie release 2.2.3
- Installation is happening on a Linux based system

## Prerequisites

The following items should be installed and configured in order to successfully
set up Genie 2.2.3.

### Required

- Java 7+
    - [Oracle](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
        - Oracle JDK is used at Netflix
    - [OpenJDK](http://openjdk.java.net/install/)
- A servlet container such as [Apache Tomcat](http://tomcat.apache.org/) for
deployment.
    - Servlet container needs to support Servlet spec 3.0+ due to use of some
    servlet 3 features
    - Genie has only been tested with Tomcat 7.x in production at Netflix, so
    your mileage may vary with other versions.
    - Tomcat 7.x has been used for the Genie docker image.

### Required if Running any Hadoop Jobs

- Binary distribution of Hadoop client
    - [Hadoop](http://hadoop.apache.org/)

### Optional

- A relational database such as [MySQL](http://www.mysql.com/) or
[PostgreSQL](http://www.postgresql.org/)
    - You don't need a standalone database. If you launch Genie with all default
    settings it will launch using an
    in memory database. Obviously this data won't persist beyond the JVM process
    shutting down but it is good for development.
    - Genie ships with MySQL 5.6 JDBC connector jars and Spring configurations
    in the WAR but you can put in your own if you want to use another database.
- Binary distributions of the clients like Hive/Pig/Presto etc., including the
command-line scripts that launch these jobs respectively.
    - You can download these packages from the project pages themselves
        - [Hive](http://hive.apache.org/)
        - [Pig](http://pig.apache.org/)
        - [Presto](https://prestodb.io/)

## Get the Genie WAR

### Download the Binary (Recommended)

The Genie releases have been uploaded to JCenter. You can download the Genie WAR
[here](https://bintray.com/netflixoss/maven/genie/2.2.3).

Click on `Files` and navigate to `genie-web/2.2.3` and download the
`genie-web-2.2.3.war` file.

### Build From Source

#### Clone the Release Tag

You should first clone the source as follows

```bash
git clone --branch 2.2.3 git@github.com:Netflix/genie.git
```

If you are having trouble cloning from GitHub, check out
[GitHub Help](https://help.github.com/articles/which-remote-url-should-i-use).

#### Build

Genie uses Gradle for builds.

Verify that your system is setup right. It should look something like this when
you run. Make sure the JVM is > 1.7.x

```bash
$ cd genie
$ ./gradlew --version

------------------------------------------------------------
Gradle 2.2.1
------------------------------------------------------------

Build time:   2014-11-24 09:45:35 UTC
Build number: none
Revision:     6fcb59c06f43a4e6b1bcb401f7686a8601a1fb4a

Groovy:       2.3.6
Ant:          Apache Ant(TM) version 1.9.3 compiled on December 23 2013
JVM:          1.8.0_45 (Oracle Corporation 25.45-b02)
OS:           Mac OS X 10.10.3 x86_64
```

Build Genie

```bash
$ ./gradlew clean build
```

A successful build should have something like this at the end

```bash
BUILD SUCCESSFUL

Total time: 1 mins 28.964 secs
```

The war will be in `genie-web/build/libs/`.

## Deploy and Configure

### Prerequisites

Assumes you've set ```CATALINA_HOME``` to be the root of your Tomcat deployment.
If not:

```
export CATALINA_HOME=/your/path/to/tomcat
```

Also if Tomcat already has a ROOT app in ```$CATALINA_HOME/webapps``` you should
move it or delete it.

### Unzip the WAR

    mkdir $CATALINA_HOME/webapps/ROOT &&\
    cd $CATALINA_HOME/webapps/ROOT &&\
    jar xf <where you downloaded or build the war>/genie-web-2.2.3.war

### Make genie-jobs and download listing formatting

    mkdir $CATALINA_HOME/webapps/genie-jobs &&\
    wget -q -P $CATALINA_HOME/conf 'https://raw.githubusercontent.com/Netflix/genie/2.2.3/root/apps/tomcat/conf/listings.xsl'

### Enable Listings in Tomcat

Enabling listings in Tomcat will allow users to view job results via their
browser.

Edit `$CATALINA_HOME/conf/web.xml` to enable listings by changing the default
servlet

```xml
<init-param>
    <param-name>listings</param-name>
    <param-value>false</param-value>
</init-param>
```

to

```xml
<init-param>
    <param-name>listings</param-name>
    <param-value>true</param-value>
</init-param>
```

Also add path to the listings.xsl file you downloaded above (replace
$CATALINA_HOME with full path on your system)

```xml
<init-param>
    <param-name>globalXsltFile</param-name>
    <param-value>$CATALINA_HOME/conf/listings.xsl</param-value>
</init-param>
```

Default servlet should look something like this when you're done

```xml
<servlet>
    <servlet-name>default</servlet-name>
    <servlet-class>org.apache.catalina.servlets.DefaultServlet</servlet-class>
    <init-param>
        <param-name>debug</param-name>
        <param-value>0</param-value>
    </init-param>
    <init-param>
        <param-name>listings</param-name>
        <param-value>true</param-value>
    </init-param>
    <init-param>
        <param-name>globalXsltFile</param-name>
        <param-value>$CATALINA_HOME/conf/listings.xsl</param-value>
    </init-param>
    <load-on-startup>1</load-on-startup>
</servlet>
```

### Modify Database Connection Settings (Optional)

If you don't want to run against the in memory database, aren't using MySQL or
your MySQL isn't running on localhost you'll need to modify the database
configuration. Genie uses [Spring](http://spring.io/) for various functionality
including the data access layer. Database connection information is stored in
the `genie-jpa.xml` file.

You'll find the file in `$CATALINA_HOME/webapps/ROOT/WEB-INF/classes`

Edit your configurations as needed. If you're not using MySQL you'll have to
change the driver class. The connection url will have to be changed if it's not
localhost. Currently password is set to nothing so if you have one configured
you should set it. Note if you want to use MySQL you'll need to change the
spring active profile at runtime which is described below.

### Update Swagger Endpoint (Optional)

Genie ships with integration with [Swagger](http://swagger.io/). By default the
Swagger JSON is at `http://localhost:7001/genie/swagger.json`. If you want your
install of Genie to support the Swagger UI, located at
`http://{genieHost}:{port}/docs/api`, you'll need to modify two things if you
want to bind the Swagger docs and JSON to anything other than localhost.

On line 19 of `$CATALINA_HOME/webapps/ROOT/WEB-INF/classes/genie-swagger.xml`
modify `localhost:7001` to match your hostname and port.

Then in `$CATALINA_HOME/webapps/ROOT/WEB-INF/lib` you'll find
`genie-server-2.2.3.jar`. Take this jar and copy it to somewhere like tmp
and unzip it. `jar xf genie-server-2.2.3.jar`. After the jar is
unzipped you'll find the documentation webpage in
`META-INF/resources/docs/api/index.html`. Modify all occurrences of `localhost`
and `7001` to match your deployment. Zip the files back up into a jar.

The whole process described above should look something like this:

```bash
GENIE_SERVER_JAR_PATH=($CATALINA_HOME/webapps/ROOT/WEB-INF/lib/genie-server-*.jar)
GENIE_SERVER_JAR_NAME=$(basename ${GENIE_SERVER_JAR_PATH})
mkdir /tmp/genie-server
mv ${GENIE_SERVER_JAR_PATH} /tmp/genie-server/
pushd /tmp/genie-server/
jar xf ${GENIE_SERVER_JAR_NAME}
rm ${GENIE_SERVER_JAR_NAME}
sed -i "s/localhost/${EC2_PUBLIC_HOSTNAME}/g" META-INF/resources/docs/api/index.html
jar cf ${GENIE_SERVER_JAR_NAME} *
mv ${GENIE_SERVER_JAR_NAME} ${GENIE_SERVER_JAR_PATH}
popd
rm -rf /tmp/genie-server
sed -i "s/localhost/${EC2_PUBLIC_HOSTNAME}/g" $CATALINA_HOME/webapps/ROOT/WEB-INF/classes/genie-swagger.xml
```

Once you've made these changes when you bring up Genie you can navigate to
`http://{genieHost}:{port}/docs/api` to begin using the Swagger UI.

### Download Genie Scripts

Genie leverages several scripts to launch and kill client processes when jobs
are submitted. You should create a directory on your system (we'll refer to this
as `GENIE_HOME`) to store these scripts and you'll need to reference
this location in the property file configuration in the next section.

Download all the Genie scripts that are used to run jobs

```bash
mkdir -p $GENIE_HOME &&\
wget -q -P $GENIE_HOME 'https://raw.githubusercontent.com/Netflix/genie/2.2.3/root/apps/genie/bin/jobkill.sh' &&\
chmod 755 $GENIE_HOME/jobkill.sh &&\
wget -q -P $GENIE_HOME 'https://raw.githubusercontent.com/Netflix/genie/2.2.3/root/apps/genie/bin/joblauncher.sh' &&\
chmod 755 $GENIE_HOME/joblauncher.sh &&\
wget -q -P $GENIE_HOME 'https://raw.githubusercontent.com/Netflix/genie/2.2.3/root/apps/genie/bin/localCleanup.py' &&\
chmod 755 $GENIE_HOME/localCleanup.py &&\
wget -q -P $GENIE_HOME 'https://raw.githubusercontent.com/Netflix/genie/2.2.3/root/apps/genie/bin/timeout3' &&\
chmod 755 $GENIE_HOME/timeout3
```

On line 228 of `joblauncher.sh` you may have to modify the hadoop conf location.
Older Hadoop distros have `$HADOOP_HOME/conf/` and newer ones seem to store
their conf files in `$HADOOP_HOME/etc/hadoop/`.

### Modify Genie Properties

Genie properties will be located in
`$CATALINA_HOME/webapps/ROOT/WEB-INF/classes/genie.properties`.

Environment specific properties will be in
`$CATALINA_HOME/webapps/ROOT/WEB-INF/classes/genie-{env}.properties`. These
properties will be loaded by [Archaius](https://github.com/Netflix/archaius)
using the `archaius.deployment.environment` property in `CATALINA_OPTS`.

You should review all the properties in the file but in particular pay attention
to the following and set them as need be for your configuration.

```java
com.netflix.genie.server.java.home
com.netflix.genie.server.hadoop.home
netflix.appinfo.port
com.netflix.genie.server.sys.home
com.netflix.genie.server.job.dir.prefix
com.netflix.genie.server.user.working.dir
com.netflix.genie.server.job.manager.yarn.command.cp
com.netflix.genie.server.job.manager.yarn.command.mkdir
com.netflix.genie.server.job.manager.presto.command.cp
com.netflix.genie.server.job.manager.presto.command.mkdir
```

For further information on customizing your Genie install see the customization
section below.

### Run Tomcat

Set CATALINA_OPTS to tell [Archaius](https://github.com/Netflix/archaius) what
properties to use as well as what Spring profile to use. By default Genie will
use dev for the Spring profile. `genie-dev.properties` will override
properties in `genie.properties` if `-Darchaius.deployment.environment=dev` is
used. Below example sets Spring profile to prod which will use the prod database
connection to MySQL (unless this was modified above).

```bash
export CATALINA_OPTS="-Dspring.profiles.active=prod
-Darchaius.deployment.applicationId=genie
-Darchaius.deployment.environment=prod"
```

If you are running in the cloud (AWS), you should also set
`-Dnetflix.datacenter=cloud`.

Start up Tomcat as follows:

```bash
$CATALINA_HOME/bin/startup.sh
```

Note that the CATALINA_OPTS environment variable must be set, and available to
the Tomcat startup script.

### Verify Genie Installation

- Genie web UI: `http://<genie_host>:<tomcat_port>`
    - You can view running jobs and registered clusters from the web console.
    - Should look something like this:
        - ![Genie Screenshot]({{ site.baseurl }}/images/2/ui.png)
- Genie Jobs: `http://<genie_host>:<tomcat_port>/genie-jobs`
    - Should list the job working directories that have been run on the node.
    Empty initially.
- [Karyon](https://github.com/Netflix/karyon) admin console:
`http://<genie_host>:8077`
    - You can view the various properties, jars, JMX metrics, etc from the
    admin console.

## Additional Configuration

### Database Configuration

Genie uses Spring for database connection setup and by default uses
[Derby](http://db.apache.org/derby/) database, which is not recommended for
production use. At Netflix, we use MySQL [RDS](http://aws.amazon.com/rds/) with
[DBCP2](http://commons.apache.org/proper/commons-dbcp/) for connection pooling.

You can look at the prod Spring profile in
[genie-jpa.xml](https://github.com/Netflix/genie/blob/2.2.3/genie-web/src/main/resources/genie-jpa.xml)
for an example on how to set up MySQL/DBCP2.

### Job Managers

Genie uses a set of classes which implement the
[Job Manager Interface](https://github.com/Netflix/genie/blob/2.2.3/genie-server/src/main/java/com/netflix/genie/server/jobmanager/JobManager.java)
, to implement the logic to run jobs on a particular Cluster type.
This usually includes setting up environment variables and other environmental
things before running a job or specific things needed to kill a job.

The
[YarnJobManagerImpl](https://github.com/Netflix/genie/blob/2.2.3/genie-server/src/main/java/com/netflix/genie/server/jobmanager/impl/YarnJobManagerImpl.java)
and
[PrestoJobManagerImpl](https://github.com/Netflix/genie/blob/2.2.3/genie-server/src/main/java/com/netflix/genie/server/jobmanager/impl/PrestoJobManagerImpl.java)
are used to run jobs on clusters of types Yarn and Presto respectively. If you
want to provide your own you can change these or implement new ones for
different cluster types. This mapping is controlled by the following properties:

#### Cluster Type to JobManager mapping
Format: com.netflix.genie.server.job.manager.<clusterType>.impl=<JobManagerImpl class>

```java
com.netflix.genie.server.job.manager.yarn.impl=com.netflix.genie.server.jobmanager.impl.YarnJobManagerImpl
com.netflix.genie.server.job.manager.presto.impl=com.netflix.genie.server.jobmanager.impl.PrestoJobManagerImpl
```

If you implement your own you'll want to assign it a type. For example lets use
Spark. You would add a property
`com.netflix.genie.server.job.manager.spark.impl` and set it to the
implementation of your class. Then when you configure your cluster for Spark
you set the `clusterType` field to be spark. At runtime when this cluster is
chosen Genie will look for the above new property and use Spring to create a
new instance for running your job.

### Copy and Mkdir Commands

Genie copies various files during job execution. Some of these files include
cluster configurations, application clients, scripts, etc. To do this the
various Job Managers set a copy command for the  
[job launcher](https://github.com/Netflix/genie/blob/2.2.3/root/apps/genie/bin/joblauncher.sh)
to use during execution. These commands by default in the
[properties file](https://github.com/Netflix/genie/blob/2.2.3/genie-web/src/main/resources/genie.properties)
are set to use the `Hadoop fs` command. If the Hadoop binaries aren't installed
on your Genie node or you'd just prefer to plug in your own functionality you
are free to do so. Your copy command just needs to take in the standard `src`
and `dst` arguments. The `mkdir` command needs to take in the path of the
directory to create.

The default properties to change are these:

```java
com.netflix.genie.server.job.manager.yarn.command.cp
com.netflix.genie.server.job.manager.yarn.command.mkdir
com.netflix.genie.server.job.manager.presto.command.cp
com.netflix.genie.server.job.manager.presto.command.mkdir
```

Internally within Netflix we actually have a custom script which interacts
directly with AWS and S3 rather than using the Hadoop commands.

If you develop your own `Job Manager` you'll want to create a copy and mkdir
command variables for that as well.

### Running Jobs as Another User

Genie runs Hadoop jobs as the user and group specified using the
HADOOP_USER_NAME environment variable. If you are running Genie on an instance
that doesn't have the users/groups, this is likely to fail. If you are
comfortable, you may have Genie add users if they don't exist already. You can
do so by un-commenting the following lines in the
[joblauncher.sh](https://github.com/Netflix/genie/blob/2.2.3r/root/apps/genie/bin/joblauncher.sh)
(Note that the user running Genie must be a sudoer for this to work):

```bash
# Uncomment the following if you want Genie to create users if they don't exist already
echo "Create user.group $HADOOP_USER_NAME.$HADOOP_GROUP_NAME, if it doesn't exist already"
sudo groupadd $HADOOP_GROUP_NAME
sudo useradd $HADOOP_USER_NAME -g $HADOOP_GROUP_NAME
```

### Eureka Integration

#### Configure Genie Server

If you have multiple Genie instances that you want to use to load-balance your
jobs, you can use [Eureka](https://github.com/Netflix/eureka) as your discovery
service. If you only have a single instance, you can safely skip this
information.

In the genie.properties, set the following property to false
`com.netflix.karyon.eureka.disable=true`, and then set up the
[eureka-client.properties](https://github.com/Netflix/genie/blob/2.2.3/genie-web/src/main/resources/eureka-client.properties)

Before starting Tomcat, also append the following to CATALINA_OPTS (assuming you
  are running in the cloud)

```bash
export CATALINA_OPTS=$CATALINA_OPTS" -Deureka.datacenter=cloud"
```

#### Configure Genie Clients

For Genie clients, you need to add a [genieClient.properties]
(https://github.com/Netflix/genie/blob/2.2.3/genie-common/src/main/resources/genie2Client.properties)
to your CLASSPATH, with the following settings (NOTE: Assumes you've named your
app genie, if you've named it genie2 change names of file and properties to
match):

```java
// Servers virtual address
genieClient.ribbon.DeploymentContextBasedVipAddresses=genie.cloud.netflix.net:<your_tomcat_port>

// Use Eureka/Discovery enabled load balancer
genieClient.ribbon.NIWSServerListClassName=com.netflix.niws.loadbalancer.DiscoveryEnabledNIWSServerList
```

Also configure the
[eureka-client.properties](https://github.com/Netflix/genie/blob/2.2.3/genie-client/src/main/resources/eureka-client.properties) as follows:

```java
// Service URLs for the Eureka server
eureka.serviceUrl.default=http://<EUREKA_SERVER_HOST>:<EUREKA_SERVER_PORT>/eureka/v2/
```

#### More Eureka Information

For more details on how to set up Eureka, please consult the Eureka
[Wiki](https://github.com/Netflix/eureka/wiki).

### Setting up Job Forwarding Between Nodes

Genie has a capability to automatically load-balance between nodes, if it has
Eureka integration enabled. If you have Eureka integration enabled,
review/update the following properties in
[genie.properties](https://github.com/Netflix/genie/blob/2.2.3/genie-web/src/main/resources/genie.properties):

```java
// max running jobs on this instance, after which 503s are thrown
com.netflix.genie.server.max.running.jobs=30

// number of running jobs on instance, after which to start forwarding to other instances
// to disable auto-forwarding of jobs, set this to higher than com.netflix.genie.server.max.running.jobs
com.netflix.genie.server.forward.jobs.threshold=20

// find an idle instance with fewer running jobs than current, by this delta
// e.g. if com.netflix.genie.server.forward.jobs.threshold=20, forward jobs to an instance
// with number of running jobs < (20-com.netflix.genie.server.idle.host.threshold.delta)
com.netflix.genie.server.idle.host.threshold.delta=5

// max running jobs on instance that jobs can be forwarded to
com.netflix.genie.server.max.idle.host.threshold=27
```

### Cloud Security

#### AWS Keys

Assuming that your data is in S3 You need to set up AWS access keys so that the
Hadoop, Hive and Pig jobs can do S3 listings, reads and writes.

If you choose to put your keys in the *-site.xml's on S3(if you are using YARN),
you may want to look into
[S3 Server Side Encryption](http://aws.typepad.com/aws/2011/10/new-amazon-s3-server-side-encryption.html) to encrypt
data at rest. You should also get and put the config files from S3 to/from your
cloud instances securely using https/ssl - EMR's S3 file system already uses
https as default (using the _fs.s3n.ssl.enabled_ property, which is set to true
by default).

Alternatively, you may simply add the `fs.s3n.awsAccessKeyId` and
`fs.s3n.awsSecretAccessKey` properties to your core-default.xml for the Hadoop
installation on the Genie server.

#### Security Groups

The Genie server needs to have access to the various daemons of your running
clusters (eg: Resource Manager for YARN clusters). Please consult the
[Amazon EC2 Security Group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
docs to enable such access.
