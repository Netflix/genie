/*
 *
 *  Copyright 2015 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.genie.client.sample

import com.google.common.collect.Multimap;
import com.netflix.config.ConfigurationManager;
import com.netflix.genie.client.ExecutionServiceClient;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.FileAttachment;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sample client demonstrating usage of the Execution Service Client.
 *
 * @author liviutudor
 */
public final class GExecutionServiceSampleClient {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionServiceSampleClient.class)

    /**
     * Main for running client code .
     *
     * @param args command line arguments
     * @throws Exception On any issue.
     */
    public static void main(final String[] args) throws Exception {

        // Initialize Eureka, if it is being used
        // LOG.info("Initializing Eureka");
        // ExecutionServiceClient.initEureka("test");
        LOG.info 'Initializing list of Genie servers'
        ConfigurationManager.getConfigInstance().setProperty('genie2Client.ribbon.listOfServers',
                'http://localhost:7001')

        LOG.info 'Initializing ExecutionServiceClient'
        def client = ExecutionServiceClient.getInstance()

        final String userName = 'genietest'
        final String jobName = 'sampleClientTestJob'
        LOG.info 'Getting jobs using specified filter criteria'
        def params = [userName: userName, status: JobStatus.FAILED.name(), limit: 3] as Multimap
        for (final Job ji : client.getJobs(params)) {
            LOG.info "Job: {id, status, finishTime} - {$ji.id, $ji.status, $ji.finished}"
        }

        LOG.info 'Running Hive job'
        def clusterCriterias = [new ClusterCriteria(['adhoc'] as Set)] as List
        def commandCriteria = ['hive'] as Set

        def fa = new FileAttachment()
        fa.with { (name, data)=['hive.q', 'select count(*) from counters where dateint=20120430 and hour=10;'.getBytes('UTF-8')] }

        def job = new Job(
                userName,
                jobName,
                "1.0",
                '-f hive.q',
                commandCriteria,
                clusterCriterias
        )
        job.with {
            (description, tags, attachments) = ['This is a test', ['testgenie', 'sample'] as Set, [fa]]
        }

        job = client.submitJob(job)

        LOG.info "Job ID: $job.id"
        LOG.info "Output URL: $job.outputURI"

        LOG.info 'Getting jobInfo by jobID'
        job = client.getJob(job.id)
        LOG.info job.toString()

        LOG.info 'Waiting for job to finish'
        job = client.waitForCompletion(job.id, 600000, 5000)
        LOG.info "Job status: $job.status" 

        LOG.info 'Killing jobs using jobID'
        def killedJob = client.killJob(job.id)
        LOG.info "Job status: $killedJob.status"

        LOG.info 'Done'
    }
}
