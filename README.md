# Genie

[![Download](https://api.bintray.com/packages/netflixoss/maven/genie/images/download.svg)](https://bintray.com/netflixoss/maven/genie/_latestVersion)
[![License](https://img.shields.io/github/license/Netflix/genie.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Issues](https://img.shields.io/github/issues/Netflix/genie.svg)](https://github.com/Netflix/genie/issues)
[![NetflixOSS Lifecycle](https://img.shields.io/osslifecycle/Netflix/genie.svg)]()

## Introduction

Genie is a federated job orchestration engine developed by Netflix. Genie provides REST-ful APIs to run a variety of big
data jobs like Hadoop, Pig, Hive, Spark, Presto, Sqoop and more. It also provides APIs for managing the metadata of many 
distributed processing clusters and the commands and applications which run on them.

## Documentation

See the official [website](https://netflix.github.io/genie) to find documentation about Genie and specific 
documentation for various releases.

## Demo

Genie has demo instructions available for all 3.x.x releases. Please see the release you're interested in demoing 
on the [releases](https://netflix.github.io/genie/releases/) page. Click on the release and then demo docs.

## Builds

Genie builds are run on Travis CI [here](https://travis-ci.org/Netflix/genie).

|  Branch |                                                     Build                                                     |                                                                         Coverage                                                                         |
|:-------:|:-------------------------------------------------------------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------:|
|  Master | [![Build Status](https://travis-ci.org/Netflix/genie.svg?branch=master)](https://travis-ci.org/Netflix/genie) |  [![Coverage Status](https://coveralls.io/repos/github/Netflix/genie/badge.svg?branch=master)](https://coveralls.io/github/Netflix/genie?branch=master)  |

## Docker

Successful builds will also generate a docker image which is published to Docker Hub. 

### App Image

[![App Image](https://img.shields.io/docker/pulls/netflixoss/genie-app.svg)](https://hub.docker.com/r/netflixoss/genie-app/)

This is the image for the Spring Boot all in one jar. You can use `docker pull netflixoss/genie-app:{version}` to test 
the one you want.
 
You can run via `docker run -t --rm -p 8080:8080 netflixoss/genie-app:{version}`

### WAR Image

[![WAR Image](https://img.shields.io/docker/pulls/netflixoss/genie-war.svg)](https://hub.docker.com/r/netflixoss/genie-war/)

This is the image that has Genie deployed as a WAR file within Tomcat. You can use 
`docker pull netflixoss/genie-war:{version}` to test the one you want.

You can run via `docker run -t --rm -p 8080:8080 netflixoss/genie-war:{version}`
