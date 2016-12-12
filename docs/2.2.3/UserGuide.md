---
layout: page
title: User Guide
teaser: User Guide for Genie 2.2.3
header: no
permalink: /releases/2.2.3/UserGuide.html
sidebar: left
---

## Introduction

This page provides details of all the API's and Client information for Genie

## Clients

### REST

#### Installation

The REST API's are available on any node running Genie at
`http://<genie_host>:<genie_port>/genie/v2/<endpoint>`.

#### Documentation

Documentation for all the available REST API's can be found
[here](http://netflix.github.io/genie/docs/2.2.3/api/). If you've
installed Genie on your localhost you can also find this documentation at
`http://<host>:<port>/genie/docs/api` and you can use the API's directly from
there. This can be modified in production deployments to work with proper host
names.

### JAVA

#### Installation

You can find binaries and dependency information for Maven, Ivy, Gradle, and
others at
[Maven Central](http://search.maven.org/#search%7Cga%7C1%7Ccom.netflix.genie).

Example for Maven:

```xml
<dependency>
    <groupId>com.netflix.genie</groupId>
    <artifactId>genie-client</artifactId>
    <version>2.2.3</version>
</dependency>
```

Example for Gradle:

```groovy
compile 'com.netflix.genie:genie-client:2.2.3'
```

#### Documentation

Client JavaDocs are available
[here](http://netflix.github.io/genie/docs/2.2.3/javadoc/client/index.html).

#### Examples

The following links points to the sample java code on how to use the Clients:

[Execution Service Client Sample](https://github.com/Netflix/genie/blob/2.2.3/genie-client/src/main/java/com/netflix/genie/client/sample/ExecutionServiceSampleClient.java)

[Application Service Client Sample](https://github.com/Netflix/genie/blob/2.2.3/genie-client/src/main/java/com/netflix/genie/client/sample/ExecutionServiceSampleClient.java)

[Command Service Client Sample](https://github.com/Netflix/genie/blob/2.2.3/genie-client/src/main/java/com/netflix/genie/client/sample/ExecutionServiceSampleClient.java)

[Cluster Service Client Sample](https://github.com/Netflix/genie/blob/2.2.3/genie-client/src/main/java/com/netflix/genie/client/sample/ExecutionServiceSampleClient.java)

### Python

#### Installation

The python client for Genie can be found on pypi
[here](https://pypi.python.org/pypi/nflx-genie-client). Or it can be installed
directly using pip.

For the latest version:

```bash
pip install nflx-genie-client
```

or if you want a specific version:

```bash
pip install nflx-genie-client==<version>
```

#### Documentation

Examples can be found on the
[pypi page](https://pypi.python.org/pypi/nflx-genie-client) for the client.
