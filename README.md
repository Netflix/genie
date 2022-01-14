# Genie

[![License](https://img.shields.io/github/license/Netflix/genie.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Issues](https://img.shields.io/github/issues/Netflix/genie.svg)](https://github.com/Netflix/genie/issues)
[![NetflixOSS Lifecycle](https://img.shields.io/osslifecycle/Netflix/genie.svg)]()

## Introduction

Genie is a federated Big Data orchestration and execution engine developed by Netflix.

Genie’s value is best described in terms of the problem it solves.

Big Data infrastructure is complex and ever-evolving.

Data consumers (Data Scientists or other applications) need to jump over a lot of hurdles in order to run a simple query:
 - Find, download, install and configure a number of binaries, libraries and tools
 - Point to the correct cluster, using valid configuration and reasonable parameters, some of which are very obscure
 - Manually monitor the query, retrieve its output

What works today, may not work tomorrow.
The cluster may have moved, the binaries may no longer be compatible, etc.

Multiply this overhead times the number of data consumers, and it adds up to a lot of wasted time (and grief!).

Data infrastructure providers face a different set of problems:
 - Users require a lot of help configuring their working setup, which is not easy to debug remotely
 - Infrastructure upgrades and expansion require careful coordination with all users


Genie is designed to sit at the boundary of these two worlds, and simplify the lives of people on either side.

A data scientist can “rub the magic lamp” and just say “Genie, run query ‘Q’ using engine SparkSQL against production data”.
Genie takes care of all the nitty-gritty details. It dynamically assembles the necessary binaries and configurations, execute the job, monitors it, notifies the user of its completion, and makes the output data available for immediate and future use.

Providers of Big data infrastructure work with Genie by making resources available for use (clusters, binaries, etc) and plugging in the magic logic that the user doesn’t need to worry about: which cluster should a given query be routed to? Which version of spark should a given query be executed with? Is this user allowed to access this data? etc.
Moreover, every job’s details are recorded for later audit or debugging.

Genie is designed from the ground up to be very flexible and customizable.
For more details visit the [official documentation](https://netflix.github.io/genie)

## Builds

Genie builds are run on Travis CI [here](https://travis-ci.com/Netflix/genie).

|     Branch     |                                                     Build                                                     |                                                                Coverage (coveralls.io)                                                                 |
|:--------------:|:-------------------------------------------------------------------------------------------------------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------:|
| master (4.2.x) | [![Build Status](https://travis-ci.com/Netflix/genie.svg?branch=master)](https://travis-ci.com/Netflix/genie) | [![Coverage Status](https://coveralls.io/repos/github/Netflix/genie/badge.svg?branch=master)](https://coveralls.io/github/Netflix/genie?branch=master) |
|     4.1.x      | [![Build Status](https://travis-ci.com/Netflix/genie.svg?branch=4.1.x)](https://travis-ci.com/Netflix/genie)  |  [![Coverage Status](https://coveralls.io/repos/github/Netflix/genie/badge.svg?branch=4.1.x)](https://coveralls.io/github/Netflix/genie?branch=4.1.x)  |
|     4.0.x      | [![Build Status](https://travis-ci.com/Netflix/genie.svg?branch=4.0.x)](https://travis-ci.com/Netflix/genie)  |  [![Coverage Status](https://coveralls.io/repos/github/Netflix/genie/badge.svg?branch=4.0.x)](https://coveralls.io/github/Netflix/genie?branch=4.0.x)  |

## Project structure

### `genie-app`
Self-contained Genie service server.

### `genie-agent-app`
Self-contained Genie CLI job executor.

### `genie-client`
Genie client interact with the service via REST API.

### `genie-web`
The main server library, can be re-wrapped to inject and override server components.

### `genie-agent`
The main agent library, can be re-wrapped to inject and override components.

### `genie-common`, `genie-common-internal`, `genie-common-external`

Internal components libraries shared by the server, agent, and client modules.

### `genie-proto`

Protobuf messages and gRPC services definition shared by server and agent.
This is not a public API meant for use by other clients.

### `genie-docs`, `genie-demo`

Documentation and demo application.

### `genie-test`, `genie-test-web`

Testing classes and utilities shared by other modules.

### `genie-ui`

JavaScript UI to search and visualize jobs, clusters, commands.

### `genie-swagger`

Auto-configuration of [Swagger](https://swagger.io/) via [Spring Fox](https://springfox.github.io/springfox/). Add
to final deployment artifact of server to enable.

## Artifacts

Genie publishes to [Maven Central](https://search.maven.org/) and [Docker Hub](https://hub.docker.com/r/netflixoss/genie-app/)

Refer to the [demo]() section of the documentations for examples.
And to the [setup]() section for more detailed instructions to set up Genie.

## Python Client

The [Genie Python client](https://github.com/Netflix/pygenie) is hosted in a different repository.

## Further info
For a detailed explanation of Genie architecture, use cases, API documentation, demos, deployment and customization guides, and more, visit the
[Genie documentation](https://netflix.github.io/genie).

## Contact

To contact Genie developers with questions and suggestions, please use [GitHub Issues](https://github.com/Netflix/genie/issues)
