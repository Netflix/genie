---
layout: page
title: Data Model
teaser: Genie 2 Data Model
header: no
permalink: /concepts/2/DataModel.html
sidebar: left
---

* TOC
{:toc}

## Introduction

The Genie data model was built to support adding new job execution clients to
your system without rebuilding or redeploying Genie. A generic data model
consisting of four entities accomplishes this. These entities are explained in
more detail below along with some examples of how they could be used.

## Entities

### Application

The first entity to talk about is an application. Applications are linked to
commands in order for binaries and configurations to be downloaded and
installed at runtime. Within Netflix this is frequently used to deploy new
clients without redeploying a Genie cluster.

An application consists of the following fields. This documentation is taken
from the Genie REST API documentation found
[here](https://netflix.github.io/genie/docs/2.2.3/api/#!/applications/createApplication).

```JSON
{
    jars (Set[string], optional): Any jars needed to run this application which will be downloaded pre use,
    tags (Set[string]): the tags associated with this application,
    envPropFile (string, optional): A file location with environment variables or other settings which will be downloaded and sourced before application used,
    status (ApplicationStatus) = ['ACTIVE' or 'DEPRECATED' or 'INACTIVE']: The current status of this application,
    configs (Set[string], optional): All the configuration files needed for this application which will be downloaded pre-use,
    name (string): The name to use,
    user (string): User who created/owns this object,
    version (string): The version number,
    id (string, optional): The unique id of this resource. If one is not provided it is created internally,
    created (string, optional): When this resource was created. Set automatically by system,
    updated (string, optional): When this resource was last updated. Set automatically by system
}
```

A concrete example of this taken from the Netflix configuration of a
[Sqoop](http://sqoop.apache.org/) application is shown below. This is the
payload that would be sent to the
[createApplication](https://netflix.github.io/genie/docs/2.2.3/api/#!/applications/createApplication)
API.

{% highlight json %}
{
  "id": "sqoop145",
  "version": "1.4.5",
  "user": "tgianos",
  "name": "sqoop",
  "status": "ACTIVE",
  "envPropFile": "s3://aS3Bucket/sqoop/sqoop145.env",
  "jars": [
    "s3://aS3Bucket/sqoop/sqoop-1.4.5.tar.gz"
  ],
  "tags": [
    "type:sqoop",
    "ver:1.4.5"
  ]
}
{% endhighlight %}

Breaking down a couple details in this example. All the Sqoop version 1.4.5
libraries were zipped up into `sqoop-1.4.5.tar.gz` and stored in S3. At runtime
when a command using this application is chosen to run a job Genie, using its
configured copy command, pulls down this zip along with the envPropFile. The
envPropFile contains a script which will unzip the downloaded zip file in the
proper place within the job working directory so everything is available in the
classpath for the command to execute. The script is sourced before the job is
run. Also a couple of tags were added to help locate the application in
searches. Below is the result of the createApplication call where Genie has
added system fields about when it was created, updated and some tags
corresponding to id and name.

{% highlight json %}
{
  "id": "sqoop145",
  "created": "2015-02-18T23:21:13Z",
  "updated": "2015-02-19T00:05:53Z",
  "version": "1.4.5",
  "user": "tgianos",
  "name": "sqoop",
  "status": "ACTIVE",
  "envPropFile": "s3://aS3Bucket/sqoop/sqoop145.env",
  "configs": [ ],
  "jars": [
    "s3://aS3Bucket/sqoop/sqoop-1.4.5.tar.gz"
  ],
  "tags": [
    "genie.id:sqoop145",
    "genie.name:sqoop",
    "type:sqoop",
    "ver:1.4.5"
  ]
}
{% endhighlight %}

Applications should be used to dynamically install clients and decouple their
deployment from Genie and are optional. You can have commands without a linked
application if the clients were pre-installed on the Genie node.

### Command

Commands can be thought of as what you would execute if you were sitting at the
command line interface and wanted to run a process on your execution cluster.
What would you execute? What dependencies would you need? What clusters can
you run the command on? This is the use case for the Command entity.

A Command consists of the following fields. This documentation is taken from
the Genie REST API documentation found
[here](https://netflix.github.io/genie/docs/2.2.3/api/#!/commands/createCommand).

```JSON
{
    tags (Set[string]): All the tags associated with this command,
    executable (string): Location of the executable for this command,
    envPropFile (string, optional): Location of a property file which will be downloaded and sourced before command execution,
    status (CommandStatus) = ['ACTIVE' or 'DEPRECATED' or 'INACTIVE']: The status of the command,
    configs (Set[string], optional): Locations of all the configuration files needed for this command which will be downloaded,
    jobType (string, optional): Job type of the command. eg: hive, pig , hadoop etc,
    name (string): The name to use,
    user (string): User who created/owns this object,
    version (string): The version number,
    id (string, optional): The unique id of this resource. If one is not provided it is created internally,
    created (string, optional): When this resource was created. Set automatically by system,
    updated (string, optional): When this resource was last updated. Set automatically by system
}
```

The most important fields in this entity are tags and the executable. The tags
will be used when a job is submitted. The job will be submitted with a set of
`command criteria` tags. This set of tags will matched against all commands
registered in the system. If a matching set is found this is the command that
will be used to execute the job. The executable is the actual command executed
in the job working directory to launch the job process. It can be relative to
the job working directory if the client is configured dynamically via an
application or absolute if a client is pre-installed on the Genie node.
`Configs` can also be added if necessary. An example of this would be the
location of a `hive-site.xml file for a hive command.

A concrete example of a command is shown below. Continuing our Sqoop example
from the [Application](#Application) section. This is the payload sent to the
[createCommand](https://netflix.github.io/genie/docs/2.2.3/api/#!/commands/createCommand)
API. After the command is registered it is linked with the Sqoop application
using the
[setApplicationForCommand](https://netflix.github.io/genie/docs/2.2.3/api/#!/commands/setApplicationForCommand)
API.

{% highlight json %}
{
  "id": "sqoop145",
  "version": "1.4.5",
  "user": "tgianos",
  "name": "sqoop",
  "status": "ACTIVE",
  "executable": "jars/bin/sqoop",
  "jobType": "sqoop",
  "tags": [
    "type:sqoop",
    "data:prod",
    "data:test",
  ]
}
{% endhighlight %}

Note that the executable is `jars/bin/sqoop`. This means that the executable is
relative to the job working directory and it's where the above Sqoop application
installed the binary via the `envPropFile`. After creation the Sqoop command
will look something like this:

{% highlight json %}
{
  "id": "sqoop145",
  "created": "2015-02-19T00:05:52Z",
  "updated": "2015-02-20T00:48:12Z",
  "version": "1.4.5",
  "user": "tgianos",
  "name": "sqoop",
  "status": "ACTIVE",
  "executable": "jars/bin/sqoop",
  "envPropFile": null,
  "jobType": "sqoop",
  "configs": [ ],
  "tags": [
    "genie.id:sqoop145",
    "genie.name:sqoop",
    "type:sqoop",
    "data:prod",
    "data:test",
  ]
}
{% endhighlight %}

You can have any number of commands configured in the system. They should then
be linked to the clusters they can execute on. Clusters are explained next.

### Cluster

A cluster stores all the details of an execution cluster including connection
information, properties, etc. Some cluster examples are Hadoop, Spark, Presto,
etc. Every cluster can be linked to a set of commands that it can run.

A Cluster consists of the following fields. This
documentation is taken from the Genie REST API documentation found
[here](https://netflix.github.io/genie/docs/2.2.3/api/#!/clusters/createCluster).

```JSON
{
    tags (Set[string]): The tags associated with this cluster,
    status (ClusterStatus) = ['UP' or 'OUT_OF_SERVICE' or 'TERMINATED']: The status of the cluster,
    clusterType (string): The type of the cluster to use to figure out the job manager for this cluster. e.g.: yarn, presto, mesos etc. The mapping to a JobManager will be specified using the property: com.netflix.genie.server.job.manager.<clusterType>.impl,
    configs (Set[string]): All the configuration files needed for this cluster which will be downloaded pre-use,
    name (string): The name to use,
    user (string): User who created/owns this object,
    version (string): The version number,
    id (string, optional): The unique id of this resource. If one is not provided it is created internally,
    created (string, optional): When this resource was created. Set automatically by system,
    updated (string, optional): When this resource was last updated. Set automatically by system
}
```

Beyond the standard set of required things (name, user, version) there are a
few important fields to pay attention to here. `Tags`, like with tags in a
command, are important for job execution. Jobs will pass in various sets of tags
which should match one of your clusters. A matching cluster and command linked
to that cluster need to be found in order to successfully launch a job.
`ClusterType` is important so Genie knows which `JobManager` to use during
execution. It is mapped via one of the
`com.netflix.genie.server.job.manager.<clusterType>.impl` properties in
`genie.properties`. `Configs` will contain any configuration files that should
be placed in the job conf directory and usually include things like the
`*-site.xml` files for Hadoop clusters.

A concrete example of a cluster is shown below. Continuing our Sqoop example
from the [Application](#Application) and [Command](#Command) sections we'll be
registering a Hadoop cluster. This is the payload sent to the [createCluster]
(https://netflix.github.io/genie/docs/2.2.3/api/#!/clusters/createCluster) API.
After the cluster is registered the Sqoop command should be linked to the
cluster via the
[addCommandsForCluster](https://netflix.github.io/genie/docs/2.2.3/api/#!/clusters/addCommandsForCluster)
API.

{% highlight json %}
{
  "id": "bdp_h2query_20150219_185356",
  "version": "2.4.0",
  "user": "tgianos",
  "name": "h2query",
  "status": "UP",
  "clusterType": "yarn",
  "configs": [
    "s3://aS3Bucket/users/bdp/h2query/20150219/185356/genie/hdfs-site.xml",
    "s3://aS3Bucket/users/bdp/h2query/20150219/185356/genie/mapred-site.xml",
    "s3://aS3Bucket/users/bdp/h2query/20150219/185356/genie/yarn-site.xml",
    "s3://aS3Bucket/users/bdp/h2query/20150219/185356/genie/core-site.xml"
  ],
  "tags": [
    "type:yarn",
    "ver:2.4.0",
    "sched:adhoc"
  ]
}
{% endhighlight %}

In this example when the cluster was brought up in EMR the four pertinent
`*-site.xml` files were uploaded to S3 so they can be downloaded by Genie.
They're added here to be saved. Also the tags are namespaced (you don't have to
do this) to show that this cluster  is an adhoc cluster which is of type yarn.
The `clusterType` field set to yarn will map to whatever class is set for the
property `com.netflix.genie.server.job.manager.yarn.impl` in
`genie.properties`. After the cluster has successfully been registered in Genie
it will look something like this:

{% highlight json %}
{
  "id": "bdp_h2query_20150219_185356",
  "created": "2015-02-20T00:47:44Z",
  "updated": "2015-02-20T00:47:44Z",
  "version": "2.4.0",
  "user": "tgianos",
  "name": "h2query",
  "status": "UP",
  "clusterType": "yarn",
  "configs": [
    "s3://aS3Bucket/users/bdp/h2query/20150219/185356/genie/hdfs-site.xml",
    "s3://aS3Bucket/users/bdp/h2query/20150219/185356/genie/mapred-site.xml",
    "s3://aS3Bucket/users/bdp/h2query/20150219/185356/genie/yarn-site.xml",
    "s3://aS3Bucket/users/bdp/h2query/20150219/185356/genie/core-site.xml"
  ],
  "tags": [
    "genie.id:bdp_h2query_20150219_185356",
    "genie.name:h2query",
    "type:yarn",
    "ver:2.4.0",
    "sched:adhoc"
  ]
}
{% endhighlight %}

Once a cluster has been linked to a command your Genie instance is ready to
start running jobs. The job entity is described in the following section. One
important thing to note is that the list of commands linked to the cluster is a
priority ordered list. That means if you have two pig commands available on your
system for the same cluster the first one found in the list will be chosen.

### Job

A job contains all the details of a job request and execution including any
command line arguments. Based on the request parameters, a cluster and command
combination is selected for execution. Job requests can also supply necessary
files to Genie either as attachments or using the file dependencies field if
they already exist in an accessible file system. As a job executes, its details
are recorded in the job record within the Genie database.

A Job consists of the following fields. This documentation is taken from the
Genie REST API documentation found
[here](https://netflix.github.io/genie/docs/2.2.3/api/#!/jobs/submitJob).

```JSON
{
    hostName (string, optional): The genie host where the job is being run or was run. Set automatically by system,
    tags (Set[string], optional): Any tags a user wants to add to the job to help with discovery of job later,
    commandArgs (string): Command line arguments for the job. e.g. -f hive.q,
    attachments (Set[FileAttachment], optional): Attachments sent as a part of job request. Can be used as command line arguments,
    commandId (string, optional): Id of the command that this job is using to run or ran with. Set automatically by system,
    fileDependencies (string, optional): Dependent files for this job to run. Will be downloaded from s3/hdfs before job starts,
    envPropFile (string, optional): Path to a shell file which is sourced before job is run where properties can be set,
    statusMsg (string, optional): A status message about the job. Set automatically by system,
    email (string, optional): Email address to send notifications to on job completion,
    outputURI (string, optional): The URI where to find job output. Set automatically by system,
    applicationName (string, optional): Name of the application that this job is using to run or ran with. Set automatically by system,
    group (string, optional): Group name of the user who submitted this job,
    status (JobStatus, optional) = ['INIT' or 'RUNNING' or 'SUCCEEDED' or 'KILLED' or 'FAILED']: The current status of the job. Set automatically by system,
    forwarded (boolean, optional): Whether this job was forwarded or not. Set automatically by system,
    processHandle (integer, optional): The process handle. Set by system,
    killURI (string, optional): The URI to use to kill the job. Set automatically by system,
    exitCode (integer, optional): The exit code of the job. Set automatically by system,
    disableLogArchival (boolean, optional): Boolean variable to decide whether job should be archived after it finishes defaults to true,
    applicationId (string, optional): Id of the application that this job is using to run or ran with. Set automatically by system,
    description (string, optional): Description specified for the job,
    executionClusterName (string, optional): Name of the cluster where the job is running or was run. Set automatically by system,
    executionClusterId (string, optional): Id of the cluster where the job is running or was run. Set automatically by system,
    commandName (string, optional): Name of the command that this job is using to run or ran with. Set automatically by system,
    started (string, optional): The start time of the job. Set automatically by system,
    finished (string, optional): The end time of the job. Initialized at 0. Set automatically by system,
    clientHost (string, optional): The hostname of the client submitting the job. Set automatically by system,
    archiveLocation (string, optional): Where the logs were archived. Set automatically by system,
    clusterCriterias (array[ClusterCriteria]): List of criteria containing tags to use to pick a cluster to run this job, evaluated in order,
    commandCriteria (Set[string]): List of criteria containing tags to use to pick a command to run this job,
    name (string): The name to use,
    user (string): User who created/owns this object,
    version (string): The version number,
    id (string, optional): The unique id of this resource. If one is not provided it is created internally,
    created (string, optional): When this resource was created. Set automatically by system,
    updated (string, optional): When this resource was last updated. Set automatically by system
}
FileAttachment {
    name (string): The name of the file,
    data (array[byte]): The bytes of the attachment
}
ClusterCriteria {
    tags (Set[string]): The tags which are ANDed together to select a viable cluster for the job
}
```

Many of these fields are optional, or set by Genie after the job has been
submitted, so don't be overwhelmed or think they all need to be set in order to
submit a job. An example is provided below showing the limited set of things
needed. The important fields are `commandArgs` which will be appended to
your [Command](#Command) executable at job launch. Additionally you can provide
file dependencies via the `fileDependencies` field if they already exist
in a file system or via `attachments` which will transfer the file(s) to the job
working directory. The set of `clusterCriterias` provide the sets of tags which
will determine which cluster this job is run on. They're sent as a list in
preference order. So the first set of tags is attempted to be matched, then the
second, then so on. This is in conjunction with the `commandCriteria` tags. In
other words in order to successfully run a job your system must have a cluster
which has, as at least a subset of its tags, one set of the tags in
`clusterCriterias` as well as a command linked to that cluster which has, at
least as a subset of its tags, the `commandCriteria` tags. It is
a mouthful but hopefully a concrete example helps.

Below is a Job object sent to the
[submitJob](https://netflix.github.io/genie/docs/2.2.3/api/#!/jobs/submitJob)
API.

{% highlight json %}
{
  "version": "NA",
  "user": "sqoop",
  "name": "O2S3.PLTFRM.TRANSFER_AUTOMIC_JOB_STATS_TO_S3_DAILY",
  "commandArgs": "command arguments for the sqoop command here",
  "clusterCriterias": [
    {
      "tags": [
        "sched:adhoc",
        "type:yarn"
      ]
    },
    {
      "tags": [
        "type:yarn"
      ]
    }
  ],
  "commandCriteria": [
    "type:sqoop",
    "ver:1.4.5"
  ],
  "tags": [
    "sqoop"
  ]
}
{% endhighlight %}

Some things to note. There are two sets of `commandCriteria`. If you look back
at the cluster registered in the Cluster section you'll see it has
the following tags:

{% highlight json %}
[
  "genie.id:bdp_h2query_20150219_185356",
  "genie.name:h2query",
  "type:yarn",
  "ver:2.4.0",
  "sched:adhoc"
]
{% endhighlight %}

It's easy to see that the first set of `clusterCriteria` will match a subset of
the Cluster tags. Now the `commandCriteria` if you look back to the
Command section you'll see it was registered with tags:

{% highlight json %}
[
  "genie.id:sqoop145",
  "genie.name:sqoop",
  "type:sqoop",
  "data:prod",
  "data:test"
]
{% endhighlight %}

type:sqoop and ver:1.4.5 were all that was requested by the job so it will match
this command and cluster combination as they were linked. So now we have the
cluster and command to run against that cluster. What should we run? Well that
was provided by the `commandArgs`. It's been taken out here since it had
Netflix data but it was standard Sqoop arguments that could be replaced by your
use case. We also tagged the job itself with ```sqoop``` so we could easily
search for it later along with other sqoop jobs. After the job is submitted
Genie will execute it and update the job object in the database. After job
completion the object will look something like this:

{% highlight json %}
{
  "id": "01259902-af71-11e4-bb44-0a03223debf4",
  "created": "2015-02-08T09:01:05Z",
  "updated": "2015-02-08T09:04:21Z",
  "version": "NA",
  "user": "sqoop",
  "name": "O2S3.PLTFRM.TRANSFER_AUTOMIC_JOB_STATS_TO_S3_DAILY",
  "commandArgs": "command arguments for the sqoop command here",
  "description": null,
  "group": null,
  "envPropFile": null,
  "clusterCriterias": [
    {
      "tags": [
        "sched:adhoc",
        "type:yarn"
      ]
    },
    {
      "tags": [
        "type:yarn"
      ]
    }
  ],
  "commandCriteria": [
    "type:sqoop",
    "ver:1.4.5"
  ],
  "fileDependencies": "",
  "attachments": null,
  "disableLogArchival": false,
  "email": null,
  "tags": [
    "sqoop"
  ],
  "executionClusterName": "h2query",
  "executionClusterId": "bdp_h2query_20150219_185356",
  "applicationName": "sqoop",
  "applicationId": "sqoop145",
  "commandName": "sqoop",
  "commandId": "sqoop145",
  "processHandle": 29266,
  "status": "SUCCEEDED",
  "statusMsg": "Job finished successfully",
  "started": "2015-02-21T09:01:05Z",
  "finished": "2015-02-21T09:04:21Z",
  "clientHost": "X.X.X.X",
  "hostName": "X.X.X.X",
  "killURI": "http://X.X.X.X:7001/genie/v2/jobs/01259902-af71-11e4-bb44-0a03223debf4",
  "outputURI": "http://X.X.X.X:7001/genie-jobs/01259902-af71-11e4-bb44-0a03223debf4",
  "exitCode": 0,
  "forwarded": false,
  "archiveLocation": "s3://aS3Bucket/genie/logs/01259902-af71-11e4-bb44-0a03223debf4"
}
{% endhighlight %}

Now all those other fields from the model are filled in. We can see the final
job status, where job results are using the `outputURI` and other important
information.

## Conclusion

Hopefully this guide provides insight into how the Genie data model is thought
out and works together. It is meant to be very generic and support as many use
cases as possible without modifications to the Genie codebase.

If you have further questions don't hesitate to reach out on the via one of the
methods on the [Contact]({{ site.baseurl }}/contact/) page.
