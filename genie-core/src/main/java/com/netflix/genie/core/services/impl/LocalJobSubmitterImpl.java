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
package com.netflix.genie.core.services.impl;


import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.jobs.JobExecutor;

import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.FileCopyService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Implementation of the Job Submitter service that runs the job locally on the same host.
 *
 * @author amsharma
 */
@Service
public class LocalJobSubmitterImpl implements JobSubmitterService {

    private static final Logger LOG = LoggerFactory.getLogger(LocalJobSubmitterImpl.class);
    private final JobPersistenceService jobPersistenceService;
    private final ClusterService clusterService;
    private final CommandService commandService;
    private final ApplicationService applicationService;
    private final ClusterLoadBalancer clusterLoadBalancer;
    @Value("${com.netflix.genie.server.user.working.dir:/mnt/tomcat/genie-jobs}")
    private String baseWorkingDirPath;
    private List<FileCopyService> fileCopyServiceImpls;

    /**
     * Constructor create the object.
     *
     * @param jps Implementation of the job persistence service
     * @param clusterService       Implementation of cluster service interface
     * @param commandService       Implementation of command service interface
     * @param applicationService   Implementation of the application service interface
     * @param fileCopyServiceImpls List of implementations of the file copy interface
     * @param clusterLoadBalancer  Implementation of the cluster load balancer interface
     */
    @Autowired
    public LocalJobSubmitterImpl(
        final JobPersistenceService jps,
        final ClusterService clusterService,
        final CommandService commandService,
        final ApplicationService applicationService,
        final ClusterLoadBalancer clusterLoadBalancer,
        final List<FileCopyService> fileCopyServiceImpls
    ) {

        this.jobPersistenceService = jps;
        this.clusterService = clusterService;
        this.commandService = commandService;
        this.applicationService = applicationService;
        this.clusterLoadBalancer = clusterLoadBalancer;
    }

    /**
     * Submit the job for appropriate execution based on environment.
     *
     * @param jobRequest of job to run
     * @throws GenieException if there is an error
     */
    @Override
    public void submitJob(
        @NotNull(message = "No job provided. Unable to submit job for execution.")
        @Valid
        final JobRequest jobRequest
    ) throws GenieException {
        LOG.debug("called with job request {}", jobRequest);

        // construct the job execution environment object for this job request
        final JobExecutionEnvironment jee = new JobExecutionEnvironment();

        try {
            jee.init(
                this.clusterService,
                this.commandService,
                this.applicationService,
                this.clusterLoadBalancer,
                jobRequest,
                baseWorkingDirPath
            );
        } catch (GeniePreconditionException gpe) {
            this.jobPersistenceService.updateJobStatus(
                jobRequest.getId(),
                JobStatus.INVALID,
                "Unable to resolve to valid cluster/command combination for criteria specified.");
            throw gpe;
        }

        // Job can be run as there is a valid cluster/command combination for it.
        // TODO figure out mode
        final JobExecutor jobExecutor = new JobExecutor(fileCopyServiceImpls, jee, "genie");
        jobExecutor.execute();

        final JobExecution jobExecution = new JobExecution.Builder(
                jee.getHostname(),
                jee.getProcessId()
            )
            .withId(jobRequest.getId())
            .build();

        this.jobPersistenceService.createJobExecution(jobExecution);

        // Change status of job to Running
        this.jobPersistenceService.updateJobStatus(jobRequest.getId(), JobStatus.RUNNING, "Job is Running");

        // Update the Cluster Information for the job
        this.jobPersistenceService.updateClusterForJob(
            jobRequest.getId(),
            jee.getCluster().getId());

        // Update the Command Information for the job
        this.jobPersistenceService.updateCommandForJob(
            jobRequest.getId(),
            jee.getCommand().getId());
    }
}
