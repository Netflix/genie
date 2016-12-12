---
layout: page
title: "About"
subheadline: "About Genie"
teaser: "High level information about Genie"
permalink: /about/
header: no
sidebar: left
---

Genie provides scalable, federated job and resource management for users of computational resources.

From the perspective of the end-user, Genie abstracts away the physical details of various (potentially transient) computational resources (like YARN, Presto, Mesos clusters etc.). It then provides APIs to submit and monitor jobs on these clusters without users having to install any clients themselves or know details of the clusters and commands.

Administrators will use the configuration APIs to register clusters and the commands/applications that run on them with Genie. The Genie nodes can have all the clients pre-installed on them or Genie will download and install them at runtime if properly configured. Users can then look up what clusters and commands are available and submit jobs to be processed. Once jobs are submitted users can query Genie for job status and output.

A big advantage of this model is the scalability that it provides for client resources. This solves a very common problem where a single machine is configured as an entry point to submit jobs to large clusters and the machine gets overloaded. Genie allows the use of a group of machines which can increase and decrease in number to handle the increasing load, providing a very scalable solution.

## Github Info

<iframe src="https://ghbtns.com/github-btn.html?user={{ site.github.owner_name }}&repo={{ site.github.repository_name }}&type=star&count=true&size=large" frameborder="0" scrolling="0" width="160px" height="30px"></iframe>
<iframe src="https://ghbtns.com/github-btn.html?user={{ site.github.owner_name }}&repo={{ site.github.repository_name }}&type=watch&count=true&size=large&v=2" frameborder="0" scrolling="0" width="160px" height="30px"></iframe>
<iframe src="https://ghbtns.com/github-btn.html?user={{ site.github.owner_name }}&repo={{ site.github.repository_name }}&type=fork&count=true&size=large" frameborder="0" scrolling="0" width="158px" height="30px"></iframe>
