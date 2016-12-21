---
layout: page
title: How Genie 2 Works
teaser: How a job is executed
header: no
permalink: /concepts/2/HowItWorks.html
sidebar: left
---

* TOC
{:toc}

The following diagram explains the core components of Genie, and its two classes
of users - administrators,and end-users.

![Genie 2 Architecture]({{ site.baseurl }}/images/2/architecture.png)

## Built on NetflixOSS

Genie itself is built on top of Netflix OSS. At its core, Genie uses the
following components:

* [Karyon](https://github.com/Netflix/karyon), which provides bootstrapping,
runtime insights, diagnostics, and various cloud-ready hooks
* [Eureka](https://github.com/Netflix/Eureka), which provides service
registration and discovery, Although Genie can run in the data-center as well
* [Archaius](https://github.com/Netflix/archaius), for dynamic property
management in the cloud
* [Ribbon](https://github.com/Netflix/Ribbon), which provides Eureka integration
, and client-side load-balancing for REST-ful interprocess communication
* [Servo](https://github.com/Netflix/Servo), which enables exporting metrics,
registering them with JMX (Java Management Extensions), and publishing them to
external monitoring systems such as Amazon's CloudWatch

## Where to Get it

Genie can be downloaded from
[Maven Central](http://repo1.maven.org/maven2/com/netflix/genie/genie-web) or
cloned from
[Github](https://github.com/Netflix/genie), built, and deployed into a container
such as [Tomcat](http://tomcat.apache.org/).

## Typical Cluster Registration at Netflix

Registration of a cluster with Genie generally follows these steps:

1. Administrators first spin up an Execution cluster, e.g. A YARN cluster using
the EMR client API.
2. Then they decide which clients are needed to run jobs on these clusters.
    1. eg: Hive/Pig clients for YARN clusters or Presto client for Presto
    cluster.
    2. The clients usually are pre-installed on the Genie node unless they can
    be run simply using a dependency file of
    some kind (eg: executable jar) which are simple to download at runtime.
3. They then upload the configurations for this cluster (*-site.xml’s) and
commands to some location on S3.
4. Next, the administrators use the Genie client to discover a Genie instance
via Eureka (for cloud deployments), and make an API call to register the cluster
and commands with Genie.
5. After the cluster and commands are registered they are linked together to let
Genie know that those commands are available to be run on this cluster.
6. Both the clusters and command are registered with sets of tags. The tags are
used by users to select the clusters and the commands to run by job submission.

## Typical Job Submission at Netflix

After a cluster has been registered, Genie is now ready to grant wishes to its
end-users - just as long as their wishes are to submit jobs to run one of the
Commands registered with Genie!

End-users use the Genie client to launch and monitor jobs. The client internally
uses Eureka to discover a live Genie instance, and Ribbon to perform client-side
load balancing, and to communicate REST-fully with the service. Users specify
job parameters, which consist of:

- Cluster Tags
- Command Tags
- Command-line arguments for the job,
- A set of file dependencies on S3 that can include scripts or UDFs (user
  defined functions).

Genie creates a new working directory for each job, stages all the dependencies
(including configurations for the chosen cluster and commands), and then forks
off a client process from that working directory. It then returns a job
ID, which can be used by the clients to query for status, and also to get an
output URI, which can be browsed during and after job execution (see below).
Users can monitor the standard output and error, and also look at jobs logs,
if anything went wrong.

![Genie 2 Output Directory]({{ site.baseurl }}/images/2/output.png)
