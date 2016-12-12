---
layout: page
title: Netflix Deployment
teaser: How Genie is Deployed at Netflix
header: no
permalink: /concepts/3/NetflixDeployment.html
sidebar: left
---

# Introduction

Many people ask how Genie is deployed at Netflix on AWS. This page tries to
explain at a high level the components used and how Genie integrates into
the environment.

# Deployment

Below is a diagram of how deployment looks at Netflix.

**Click image for full size**.

[![Genie Netflix Deployment]({{ site.baseurl }}/images/3/deployment.png)]({{ site.baseurl }}/images/3/deployment.png)

## Components

Brief descriptions of all the components.

### Elastic Load Balancer

The [Elastic Load Balancer](https://aws.amazon.com/elasticloadbalancing/) (ELB)
is used for a few purposes.

- Allow a single endpoint for all API calls
- Distribute API calls amongst all Genie nodes in an ASG
- Allow HTTPS termination at single point
- Allow human friendly DNS name to be assigned via
[Route 53](https://aws.amazon.com/route53/) entry

### Auto Scaling Group (ASG)

A cluster of Genie nodes. Each
[AMI](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html)
contains an Apache HTTP server fronting
Tomcat 8 via [AJP](http://tomcat.apache.org/connectors-doc/index.html).

Currently the Genie ASG is a fleet of
[i2.4xl](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/i2-instances.html)
instances. The primary production ASG sizes about thirty instances at any given
time. Each Genie instance is configured to be allocated 80% of the available
system memory for jobs. Tomcat itself is given 10 GB. Leaving the rest for the
system and other processes.

The ASG is set to auto scale when the average amount of used job memory, that
80% of the system memory, exceeds 60% of the available.

For example an i2.4xl image has 122 GB of available memory. For simplicity we
allocate 100 GB for jobs. If the average memory used for jobs per node across
the ASG exceeds 60 GB for some period of time we will scale the cluster up by
one node to pre-emptively allocate resources before we get in trouble.

Currently we don't auto scale down but from time to time we take a look to see
if a new ASG needs to be launched at a smaller size.

### Relational Database (RDS)

We currently use an [Amazon Aurora](https://aws.amazon.com/rds/aurora/) cluster
on db.r3.4xl instances. Aurora is MySQL compatible so we use the standard MySQL
JDBC driver that is packaged with Genie to talk to the database. We deploy to
a Multi-AZ cluster and we have a reader endpoint that we use for reporting and
backup.

### Zookeeper

We use an [Apache Zookeeper](https://zookeeper.apache.org/) cluster which is
deployed and managed by another team within Netflix for leadership election
within our Genie ASG. When the Genie ASG comes up it
(using Spring Cloud Cluster) looks in Zookeeper to see if there already is a
leader for the app/cluster/stack combination. If there isn't it elects a new
one.

### ElastiCache

We use [AWS ElastiCache](https://aws.amazon.com/elasticache/) to provide a
Redis cluster to store our HTTP sessions (via Spring Session). This allows us
to have the users only sign in via SAML one time and not have to do it every
time the ELB routes them to a new host for the UI.

### Security (not pictured)

Internally Genie is security via OAuth2 (for APIs) and SAML (for UI). We
integrate with a Ping Federate IDP service to provide authentication and
authorization information.

HTTPS is enabled to the ELB via a Verisign signed certificate tied to the
Route 53 DNS address.

## Spinnaker

Genie is deployed using [Spinnaker](http://www.spinnaker.io/). We currently have
a few stacks (prod, test, dev, load, stage, etc) that we use for different
purposes. Spinnaker handles all this for us after we configure the pipelines.
See the Spinnaker site for more information.
