# Genie

[![Download](https://api.bintray.com/packages/netflixoss/maven/genie/images/download.svg)]
(https://bintray.com/netflixoss/maven/genie/_latestVersion)

## In Active Development

This branch contains code in active development towards Genie 3.0. It is not yet ready for use. If you're looking for 
a version that is ready for production please see the [master](https://github.com/Netflix/genie/tree/master) branch. 
If you want to see what we're working on see the [3.0.0 Milestone](https://github.com/Netflix/genie/milestones/3.0.0).

## Introduction

Genie is a federated job execution engine developed by Netflix. Genie provides REST-ful APIs to run a variety of big
data jobs like Hadoop, Pig, Hive, Spark, Presto, Sqoop and more. It also provides APIs for managing many distributed
processing cluster configurations and the commands and applications which run on them.

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

Please see the [ZeroToDocker](https://github.com/Netflix-Skunkworks/zerotodocker) project for [instructions]
(https://github.com/Netflix-Skunkworks/zerotodocker/wiki/Genie) of an end to end example in [Docker]
(https://www.docker.com/).

## API Validation

[![Swagger Validation](http://online.swagger.io/validator?url=http://netflix.github.io/genie/docs/rest/swagger.json)]
(http://netflix.github.io/genie/docs/rest/swagger.json)

## Builds

Genie builds are hosted on CloudBees [here](https://netflixoss.ci.cloudbees.com/job/NetflixOSS/job/genie/).

|        Build       |                                                                                                   Status                                                                                                  |
|:------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|      Snapshots     |      [![Build Status](https://netflixoss.ci.cloudbees.com/job/NetflixOSS/job/genie/job/genie-snapshot/badge/icon)](https://netflixoss.ci.cloudbees.com/job/NetflixOSS/job/genie/job/genie-snapshot/)      |
|    Pull Requests   | [![Build Status](https://netflixoss.ci.cloudbees.com/job/NetflixOSS/job/genie/job/genie-pull-requests/badge/icon)](https://netflixoss.ci.cloudbees.com/job/NetflixOSS/job/genie/job/genie-pull-requests/) |
|      Releases      |       [![Build Status](https://netflixoss.ci.cloudbees.com/job/NetflixOSS/job/genie/job/genie-release/badge/icon)](https://netflixoss.ci.cloudbees.com/job/NetflixOSS/job/genie/job/genie-release/)       |

## Support

Please use the [Google Group](https://groups.google.com/d/forum/genieoss) for general questions and discussion.

## Issues

You can request bug fixes and new features [here](https://github.com/Netflix/genie/issues).



