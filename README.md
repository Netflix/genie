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

## Example

[![Docker Example](https://img.shields.io/docker/pulls/netflixoss/genie.svg)](https://hub.docker.com/r/netflixoss/genie/)

Please see the [ZeroToDocker](https://github.com/Netflix-Skunkworks/zerotodocker) project for [instructions]
(https://github.com/Netflix-Skunkworks/zerotodocker/wiki/Genie) of an end to end example in [Docker]
(https://www.docker.com/).

## Support

Please use the [Google Group](https://groups.google.com/d/forum/genieoss) for general questions and discussion.

## Issues

You can request bug fixes and new features [here](https://github.com/Netflix/genie/issues).



