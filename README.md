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
1. Open a terminal and navigate to a directory where you will clone Genie
2. Clone the repository
    * `git clone https://github.com/Netflix/genie.git` or `git clone git@github.com:Netflix/genie.git` if you have 
    ssh keys already setup with Github
3. Go to the root of the repo
    * `cd genie`
4. Start the demo containers
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
            * UI's Exposed
            
            |         UI         |           Prod           |           Test           |
            |:------------------:|:------------------------:|:------------------------:|
            |  Resource Manager  |  `http://localhost:8088` |  `http://localhost:8089` |
            | Job History Server | `http://localhost:19888` | `http://localhost:19889` |
            |      NameNode      | `http://localhost:50070` | `http://localhost:50071` |
            |      DataNode      | `http://localhost:50075` | `http://localhost:50076` |
    * **Wait a while after the build says SUCCEEDED. You'll know how long once `http://localhost:8080` shows the 
    Genie UI**
5. Check out the Genie UI
    * In a browser navigate to the Genie UI (`http://localhost:8080`) and notice there are no `Jobs`, `Clusters`, 
    `Commands` or `applications` currently
    * These are available by clicking on the tabs in the top left of the UI
6. Initialize the System
    * Back in the terminal initialize the configurations for the two clusters (prod and test), 5 commands (hadoop, 
    hdfs, yarn, spark-submit, spark-shell) and two application (hadoop, spark)
    * `./gradlew demoInit`
7. Verify Configurations Loaded
    * In the browser browse the Genie UI again and verify that now `Clusters`, `Commands` and `Applications` have data 
    in them
8. Run some jobs. 
    * Recommend running the Hadoop job first so others have something interesting to show. 
    * Sub in the environment (env) for the Gradle commands below (`Prod` or `Test`)
    * Available jobs include:
    
    |    Job    |              Gradle Command             |                                                  Description                                                 |
    |:---------:|:---------------------------------------:|:------------------------------------------------------------------------------------------------------------:|
    |   Hadoop  | `./gradlew demoRun{env}HadoopJob`       | Runs grep against input directory in HDFS                                                                    |
    |    HDFS   | `./gradlew demoRun{env}HDFSJob`         | Runs a `dfs -ls` on the input directory on HDFS and stores results in stdout                                 |
    |    YARN   | `./gradlew demoRun{env}YarnJob`         | Lists all yarn applications from the resource manager into stdout                                            |
    |   Spark   | `./gradlew demoRun{env}SparkSubmitJob`  | Runs the SparkPi example for Spark 1.6.x with input of 10. Results stored in stdout                          |
    | Spark 2.x | `./gradlew demoRun{env}Spark2SubmitJob` | Overrides default Spark with Spark 2.0.x and runs SparkPi example with input of 10. Results stored in stdout |
9. For each of these jobs you can see their status, output and other information via the UI's
    * In the jobs tab you can see all the job history in the Genie UI `Jobs` tab
        * Clicking any row will expand that job information and provide more links
        * Clicking the folder icon will bring you to the working directory for that job
    * Go to the respective cluster Resource Manager UI's and verify the jobs ran on their respective cluster
10. Move load from prod to test
    * Lets say there is something wrong with the production cluster. You don't want to interfere with users but you 
    need to fix the prod cluster. Lets switch the load over to the test cluster temporarily using Genie
    * In terminal switch the prod tag `sched:sla` from Prod to Test cluster
    * `./gradlew demoMakeTestProd`
    * Verify in Genie UI `Clusters` tab that the `sched:sla` tag only appears on the `GenieDemoTest` cluster
11. Run more of the above jobs
    * Verify that all jobs went to the `GenieDemoTest` cluster and none went to the `GenieDemoProd` cluster regardless 
    of which `env` you passed into the Gradle commands above
12. Reset the system
    * You've resolved the issues with your production cluster. Move the `sched:sla` tag back
    * `./gradlew demoResetProd`
    * Verify in Genie UI `Clusters` tab that `sched:sla` tag only appears on `GenieDemoProd` cluster
13. Run some jobs
    * Verify jobs are again running on `Prod` and `Test` cluster based on environment
14. Explore the clients
    * You can see the script backing the Gradlew commands in the source code in 
    `genie-demo/src/main/docker/client/example`
    * If you want to mess around with your own scrips feel free to log into the container 
        * `docker exec -it docker_genie-client_1 /bin/bash`
            * This will put you in the working directory where you can execute your own python scripts
15. Shut it down 
    * Once you're done trying everything out you can shut down the demo
    * `./gradlew demoStop`
    * This will stop and remove all the containers from the demo. The images will remain on disk and if you run
    the demo again it will startup much faster since nothing needs to be downloaded or built

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



