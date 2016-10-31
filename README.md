# Genie

[![Download](https://api.bintray.com/packages/netflixoss/maven/genie/images/download.svg)]
(https://bintray.com/netflixoss/maven/genie/_latestVersion)
[![License](https://img.shields.io/github/license/Netflix/genie.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Issues](https://img.shields.io/github/issues/Netflix/genie.svg)](https://github.com/Netflix/genie/issues)

## In Active Development

This branch contains code in active development towards Genie 3.0. It is not yet ready for use. If you're looking for
a version that is ready for production please see the [master](https://github.com/Netflix/genie/tree/master) branch.
If you want to see what we're working on see the [3.0.0 Milestone](https://github.com/Netflix/genie/milestones/3.0.0).

## Introduction

Genie is a federated job execution engine developed by Netflix. Genie provides REST-ful APIs to run a variety of big
data jobs like Hadoop, Pig, Hive, Spark, Presto, Sqoop and more. It also provides APIs for managing many distributed
processing cluster configurations and the commands and applications which run on them.

## Builds

Genie builds are run on Travis CI [here](https://travis-ci.org/Netflix/genie).

|  Branch |                                                     Build                                                     |                                                                         Coverage                                                                         |
|:-------:|:-------------------------------------------------------------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------:|
|  Master | [![Build Status](https://travis-ci.org/Netflix/genie.svg?branch=master)](https://travis-ci.org/Netflix/genie) |  [![Coverage Status](https://coveralls.io/repos/github/Netflix/genie/badge.svg?branch=master)](https://coveralls.io/github/Netflix/genie?branch=master)  |
| Develop | [![Build Status](https://travis-ci.org/Netflix/genie.svg?branch=master)](https://travis-ci.org/Netflix/genie) | [![Coverage Status](https://coveralls.io/repos/github/Netflix/genie/badge.svg?branch=develop)](https://coveralls.io/github/Netflix/genie?branch=develop) |

## Docker

[![Docker Example](https://img.shields.io/docker/pulls/netflixoss/genie-app.svg)](https://hub.docker.com/r/netflixoss/genie-app/)

Successful builds which generate SNAPSHOT, release candidate (rc) or final artifacts also generate a docker container 
which is published to Docker Hub. You can use `docker pull netflixoss/genie-app:{version}` to test the one you want.
 
You can run via `docker run -t --rm -p 8080:8080 netflixoss/genie-app:{version}`

## Demo

A demo of Genie 3 exists as part of the source code in this repository. This demo is a work in progress.

### Prerequisites:

* Java 8
* [Docker](https://docs.docker.com/engine/installation/) (tested against v1.12.1)
* [Docker Compose](https://docs.docker.com/compose/install/)
* Disk Space
    * Four total images currently sizing ~3.3 GB
* Available Ports on local machine
    * 8080, 8088, 19888, 50070, 50075, 8089, 19889, 50071, 50076
    
### Caveats

* Since all this is running locally on one machine it can be slow, much slower than you'd expect production level
systems to run
* Networking is kind of funky within the Hadoop UI due to how DNS is working between the containers. Sometimes if you 
click a link in the UI and it doesn't work try swapping in localhost for the hostname instead.

### Steps:
* Clone the repository
    * `git clone git@github.com:Netflix/genie.git` or `git clone https://github.com/Netflix/genie.git`
* Go to the root of the repo
    * `cd genie`
* Start the demo
    * `./gradlew demoStart`
    * The first time you run this it could take quite a while as it has to download 2 large images (Genie itself 
    and Hadoop) and build two others (a genie-apache image for serving files and a genie-client)
    * This will use docker compose to bring up 5 containers with tags (name):
        * netflixoss/genie-app:{version} (docker_genie_1)
            * Image from official Genie build which runs Genie app server
            * Maps port 8080 for Genie UI
        * netflixoss/genie-demo-apache:{version} (docker_genie-apache_1)
            * Extension of apache image which includes files used during demo that Genie will download
        * netflixoss/genie-demo-client:{version} (docker_genie-client_1)
            * Simulates a client node for Genie which includes several python scripts to configure and run jobs on Genie
        * sequenceiq/hadoop-docker:2.7.1 (docker_genie-hadoop-prod_1 and docker_genie-hadoop-test_1)
            * Two Hadoop "clusters" one designated prod and one designated test
            * Ports Exposed (prod/test)
                * 8088/8089 Resource Manager UI
                * 19888/19889 Job History UI
                * 50070/50071 NameNode (HDFS) UI
                * 50075/50076 DataNode UI
    * Wait a while after the build says SUCCEEDED. You'll know how long once `http://localhost:8080` shows the Genie UI
* Look at the Genie UI (`http://localhost:8080`) and notice there are no jobs, clusters, commands or applications 
currently
* Initialize the configurations for the two clusters (prod and test), three commands (hadoop, hdfs, yarn) and single
application (hadoop)
    * `./gradlew demoInit`
* Review the Genie UI again and notice that now clusters, commands and applications have data in them
* Run some jobs. Recommend running the Hadoop job first so others have something interesting to show. 
Available jobs include:
    * `./gradlew demoRunProdHadoopJob` or `./gradlew demoRunTestHadoopJob`
        * See the MR job at `http://localhost:8088` or `http://localhost:8089` respectively
    * `./gradlew demoRunProdHDFSJob` or `./gradlew demoRunTestHDFSJob`
        * Runs a `dfs -ls` on the input directory on HDFS and stores results in stdout
    * `./gradlew demoRunProdYarnJob` or `./gradlew demoRunTestYarnJob`
        * Lists all yarn applications from the resource manager into stdout
    * `./gradlew demoRunProdSparkSubmitJob` or `./gradlew demoRunTestSparkSubmitJob`
        * Runs the SparkPi example with input of 10. Results stored in stdout
* For each of these jobs you can see their status, output and other information via the Genie UI
* For how everything is configured and run you can view the scripts in `genie-demo/src/main/docker/client/example`
* Once you're done trying everything out you can shut down the demo
    * `./gradlew demoStop`
    * This will stop and remove all the containers from the demo. The images will remain on disk and if you run
    the demo again it will startup much fasters since nothing needs to be downloaded or built.

## Documentation

* Netflix Tech Blog Posts
    * [Genie 1](http://techblog.netflix.com/2013/06/genie-is-out-of-bottle.html)
    * [Genie 2](http://techblog.netflix.com/2014/11/genie-20-second-wish-granted.html)
    * [All Posts](http://techblog.netflix.com/search/label/big%20data) from the Big Data Team at Netflix
* Presentations
    * Netflix OSS Meetups
        * Season 3 Episode 1
            * [Slides](http://www.slideshare.net/RuslanMeshenberg/netflixoss-meetup-season-3-episode-1/24)
            * [YouTube](http://youtu.be/hi7BDAtjfKY?t=15m53s)
    * [2013 Hadoop Summit](http://www.slideshare.net/krishflix/genie-hadoop-platform-as-a-service-at-netflix)
* Genie Github
    * [Wiki](https://github.com/Netflix/genie/wiki)
    * [Source](https://github.com/Netflix/genie/tree/master)
* Client API Documentation
    * [REST](http://netflix.github.io/genie/docs/api/)
    * [Python](https://pypi.python.org/pypi/nflx-genie-client)
    * [Java](http://netflix.github.io/genie/docs/javadoc/client/index.html)

## Support

Please use the [Google Group](https://groups.google.com/d/forum/genieoss) for general questions and discussion.

## Issues

You can report bugs and request new features [here](https://github.com/Netflix/genie/issues). Pull requests are always 
welcome.



