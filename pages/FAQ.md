---
layout: page
title: "FAQ"
permalink: "/faq/"
header: no
sidebar: left
---

## Introduction

Below are some questions we've frequently been asked about Genie. If you have a
question you don't see here or elsewhere on the site feel free to submit a
question to the
[Google Group](https://groups.google.com/forum/#!forum/genieoss).

### Any Version

#### Is Genie a scheduler?

Genie is not a workflow scheduler, such as [Oozie](http://oozie.apache.org/).
Genie’s unit of execution is a single job. Genie doesn’t schedule or run
workflows. At Netflix we use an enterprise scheduler
[Automic](http://www.automic.com/en) to run our ETL. Genie is not a task
scheduler, such as the Hadoop fair share or capacity schedulers either.
We think of Genie as a resource match-maker, since it matches a user job to
an appropriate cluster based on the job parameters and cluster properties.

#### Does Genie provision, launch and manage clusters?

Genie is not a resource manager per se - it doesn’t provision or launch
clusters, and neither does it scale clusters up and down based on their
utilization. Instead, Genie is a repository of clusters, commands and
applications with API's for configuration and job execution. Genie matches jobs
to the appropriate clusters based on their properties. If there are multiple
clusters that are candidates to run a job, Genie chooses a cluster at random -
however, a custom load balancer may be written to choose a cluster more
intelligently.

#### Does Genie only work in the cloud?

No. As the demo for any 3.x version of Genie shows it can run without any cloud
provider in the loop. It's also not tied to any specific cloud provider if you
are using one.

#### How much latency does Genie add?

Less than a second to a few seconds at most. This is because all the job
dependencies have to be uploaded/download from somewhere to the working
directory, and job logs have can be archived to after job submission is
complete.

#### Is it good to run "select-*" type queries in Genie?

Even though you can run "select *"-type queries which generate results in the
standard output, it is not encouraged if those queries are likely to generate
lots of data (greater than a few MB). If you plan on writing a lot of data to
standard out it is recommended you either mount a large drive for the
genie-jobs directory or write the results somewhere like HDFS or S3 in your
query.

#### Are jobs idempotent?

The Execution Service can trigger jobs which may write to tables or S3 paths -
re-running the same jobs may lead to inconsistencies. One scenario that can
lead to inconsistencies is if a client makes a REST call and dies before
receiving a response. To avoid this scenario, clients can generate a unique ID
and pass it to the Jobs API along with their request. If a job is already
running with this ID Genie will not rerun it and simply return an HTTP 409 error
(for conflict).

#### Are user and group required when submitting a job?

The `user` parameter is required for job submissions jobs will be run as the
specified user if the admin enabled this setting. The `group` is
optional.

#### Do I need to escape quotes in my command line arguments?

If you have quotes in your command-line arguments, you have to escape it. An
example of a valid JSON payload string with quotes in the command-line arguments
is as follows:

{
  "name": "MY_HIVE_JOB",
  "user" : "skrishnan",
  "group" : "netflix",
  "commandArgs": "-e \\"use default; show tables\\"",
  ...
}

#### How are jobs killed when I call send a DELETE to the job API?

Job destruction behaves differently depending on the type of job being launched.
A kill API call on the Job sends a `SIGINT` signal to the client of the
job. The behavior will depend on how the client handles it (e.g. the hive
client).

### 2.x Questions

#### I get a 404 error when I hit a job's output URI? How do I fix this?

Most likely you don't have Tomcat's directory listing enabled. Follow
instructions from the [Setup Guide]
(https://github.com/Netflix/genie/wiki/Setup#Enable-Listings-in-Tomcat).

#### Where are my logs?

Look inside the outputURI of the job. All the job status information ends up in
the standard error log. If you end up with a zero-byte stdout log and the job
claims it is succeeded, you may just have run a query with no results - you can
look into the stderr logs to confirm.

In summary, you should look at the stderr logs for the status of a running job
during execution (to figure out the % done).

#### Why am I seeing javax.el errors when I try to run my application which uses
the Genie client?

Genie uses [Hibernate Validator](http://hibernate.org/validator/) for bean
validation [Issue](https://github.com/Netflix/genie/issues/102). Since Genie
(and its client) generally run in a servlet container we intentionally follow
the model set by Hibernate for how it handles the Java Expression Langauge
implementation. See the [Hibernate Getting Started]
(http://hibernate.org/validator/documentation/getting-started/) for their take
on the issue. To fix your issue if you're running your application outside of a
web container like Tomcat add a Expression Language implementation to your
classpath as in those instructions and you should be fine.
