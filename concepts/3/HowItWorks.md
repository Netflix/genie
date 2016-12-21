---
layout: page
title: How Genie Works
teaser: Learn how Genie runs a job
header: no
permalink: /concepts/3/HowItWorks/
sidebar: left
---

* TOC
{:toc}

## Introduction

This page is meant to provide context for how Genie can be configured
with Clusters, Commands and Applications (see:
  [Data Model]({{ site.baseurl }}/concepts/3/DataModel.html)) and then how
these work together in order to run a job on a Genie node.

## Configuration Explained

This section describes how configuration of Genie works from an administrator
point of view. This isn't how to install and configure the Genie application
itself. Rather it is how to configure the various resources involved in
running a job.

### Register Resources

All resources (clusters, commands, applications) should be registered with
Genie before attempting to link them together. Any files these resources depend
on should be uploaded somewhere Genie can access them (S3, web server, mounted
  disk, etc).

Tagging of the resources (particularly Clusters and Commands) is extremely
important. Genie will use the tags in order to find a cluster/command
combination to run a job. You should come up with a convenient tagging scheme
for your organization. At Netflix we try to stick to a pattern for tags
structures like `{tag category}:{tag value}`. For example `type:yarn` or
`data:prod`. This allows the tags to have some context so that when users look
at what resources are available they can find what to submit their jobs with so
it is routed to the correct cluster/command combination.

### Linking Resources

Once resources are registered they should be linked together. By linking we mean
to represent relationships between the resources.

#### Commands for a Cluster

Adding commands to a cluster means that the administrator acknowledges that this
cluster can run a given set of commands. If a command is not linked to a cluster
it cannot be used to run a job.

The commands are added in priority order. For example say you have different
Spark commands you want to add to a given YARN cluster but you want Genie to
treat one as the default. Here is how those commands might be tagged:

Spark 1.6.0 (id: spark16)
- `type:sparksubmit`
- `ver:1.6`
- `ver:1.6.0`

Spark 1.6.1 (id: spark161)
- `type:sparksubmit`
- `ver:1.6.1`

Spark 2.0.0 (id: spark200)
- `type:sparksubmit`
- `ver:2.0`
- `ver:2.0.0`

Now if we added the commands to the cluster in this order: `spark16, spark161,
spark200` and a user submitted a job only requesting a command tagged with
`type:sparksubmit` (as in they don't care what version just the default) they
would get Spark 1.6.0. However if we later deemed 2.0.0 to be ready to be the
default we would reorder the commands to `spark200, spark16, spark161` and
that same job if submitted again would now run with Spark 2.0.0.

#### Applications for a Command

Linking application(s) to commands means that a command has a dependency on said
application(s). The order of the applications added is important because Genie
will setup the applications in that order. Meaning if one application depends
on another (e.g. Spark depends on Hadoop on classpath for YARN mode) Hadoop
should be ordered first. All applications must successfully be installed before
Genie will start running the job.

## Job Submission Explained

OK. The system admin has everything registered and linked together. Things could
obviously change but that's mostly transparent to end users. They just want to
run jobs.How does that work? This section attempts to walk through what happens
at a high level. The example section lower down will walk through a step by step
example.

### User Submits a Job Request

In order to submit a job request there is some work a user will have to do
up front. What kind of job are they running? What cluster do they want to run
on? What command do they want to use? Do they care about certain details like
version or just want the defaults? Once they determine the answers to the
questions they can decide how they want to tag their job request for the
`clusterCriterias` and `commandCriteria` fields.

General rule of thumb for these fields is to use the lowest common denominator
of tags to accomplish what a user requires. This will allow the most flexibility
for the job to be moved to different clusters or commands as need be. For
example if they want to run a Spark job and don't really care about version
it is better to just say "type:sparksubmit" (assuming this is tagging structure
at your organization) only instead of that **and** "ver:2.0.0". This way when
version 2.0.1 or 2.1.0 comes down the pipe the job moves along with the new
default. Obviously if they do care about version they should set it or any other
specific tag.

The `clusterCriterias` field is an array of `ClusterCriteria` objects. This is
done to provide a fallback mechanism. If the no match is found for the first
`ClusterCriteria` and `commandCriteria` combination it will move onto the second
and so on until all options are exhausted. This is handy if it is desirable to
run a job on some cluster that is only up some of the time but other times it
isn't and its fine to run it on some other cluster that is always available.

Other things a user needs to consider when submitting a job. All dependencies
which aren't sent as attachments must already be uploaded somewhere Genie can
access them. Somewhere like S3, web server, shared disk, etc.

Users should familiarize themselves with whatever the `executable` for their
desired command includes. It's possible the system admin has setup some default
parameters they should know are there so as to avoid duplication or unexpected
behavior. Also they should make sure they know all the environment variables
that may be available to them as part of the setup process of all the cluster,
command and application setup processes.

### Genie Receives the Job Request

When Genie receives the job request it does a few things immediately:

1. If the job request doesn't have an id it creates a GUID for the job
2. It saves the job request to the database so it is recorded
  - If the ID is in use a 409 will be returned to the user saying there is a
  conflict
3. It creates job and job execution records in data base for consistency
4. It saves any attachments in a temporary location

Next Genie will attempt to find a cluster and command matching the requested
tag combinations. If none is found it will send a failure back to the user and
mark the job failed in the database.

If a combination is found Genie will then attempt to determine if the node can
run the job. By this it means it will check the amount of client memory the job
requires against the available memory in the Genie allocation. If there is
enough the job will be accepted and will be run on this node and the jobs
memory is subtracted from the available pool. If not it will
be rejected with a 503 error message and user should retry later.

The order of priority for how memory for a job is determined is:

1. The memory a user requested in the job request
  - Cannot exceed the max memory allowed by system admin for a given job
2. The memory set as the default for a given command by the admins
3. The default memory size for a job as set by the system admin

Succesful job submission results in a 202 message to the user stating it's
accepted and will be processed asynchronously by the system.

### After Job Accepted

Once a job has been accepted to run on a Genie node a workflow is executed
in order to setup the job working directory and launch the job. Some minor steps
left out for brevity.

1. Job is marked in `INIT` state in the database
2. A job directory is created under the admin configured jobs directory with
a name of the job id
3. A run script file is created with the name `run` under the job working
directory
  - Currently this is a bash script
4. Kill handlers are added to the run script
5. Directories for Genie logs, application files, command files, cluster files
are created under the job working directory
6. Default environment variables are added to the run script to export their
values
7. Cluster configuration files are downloaded and stored in the job work
directory
8. Cluster related variables are written into the run script
9. Application configuration and dependency files are downloaded and stored in
the job directory if any applications are needed
10. Application related variables are written into the run script
11. Command configuration and dependency files are downloaded and store in the
job directory
12. Command related variables are written into the run script
13. All job dependency files (including attachments) are downloaded into the
job working directory
14. Job related variables are written into the run script
15. Job script is executed in a forked process.
16. Script `pid` stored in database `job_executions` table and job marked as
`RUNNING` in database
17. Monitoring process created for pid

### After Job Running

Once the job is running Genie will poll the PID periodically waiting for it to
no longer be used.

- Assumption made as to the amount of process churn on the
Genie node. We're aware PID's can be reused but reasonably this shouldn't
happen within the poll period given the amount of available PID to the
processes a typical Genie node will run.

Once the pid no longer exists Genie checks the done file for the exit code.
It marks the job succeeded, failed or killed depending on that code.

### Cleaning Up

To save disk space Genie will delete application dependencies from the job
working directory after a job is completed. This can be disabled by an admin.
If the job is marked as it should be archived the working directory will be
zipped up and stored in the default archive location under the {jobId}.tar.gz.

### User Behavior

Users can check on the status of their job using the `status` API and get the
output using the output API's. See the
[REST Documentation]({{ site.baseurl }}/releases/) for the specific release in
question for specifics on how to do that.

## Example

See the [example]({{ site.baseurl }}/concepts/3/HowItWorks/NetflixExample.html)
for a more concrete example of these concepts.

## Demo

See the [Demo Documentation]({{ site.baseurl }}/releases/) for a specific
release for instructions on running the Genie demo for that release which will
have specific hands on examples you can run from your own machine.
