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


import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.jobs.workflow.impl.SimpleContext;
import com.netflix.genie.core.jobs.workflow.WorkflowExecutor;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.core.services.FileCopyService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;

/**
 * Implementation of the Job Submitter service that runs the job locally on the same host.
 *
 * @author amsharma
 */
@Service
@Slf4j
public class LocalJobSubmitterImpl implements JobSubmitterService {

    private static final String JOB_EXECUTION_ENV_KEY = "jee";
    private static final String JOB_EXECUTION_DTO_KEY = "jexecdto";

    private final JobPersistenceService jobPersistenceService;
    private final ClusterService clusterService;
    private final CommandService commandService;
    private final ApplicationService applicationService;
    private final ClusterLoadBalancer clusterLoadBalancer;

    @Resource(name = "taskList")
    private List<WorkflowTask> jobWorkflowTasks;

    @Value("${com.netflix.genie.server.user.working.dir:/mnt/tomcat/genie-jobs}")
    private String baseWorkingDirPath;
    private List<FileCopyService> fileCopyServiceImpls;
    private final WorkflowExecutor wfExecutor;

    /**
     * Constructor create the object.
     *
     * @param jps                  Implementation of the job persistence service
     * @param clusterService       Implementation of cluster service interface
     * @param commandService       Implementation of command service interface
     * @param applicationService   Implementation of the application service interface
     * @param fileCopyServiceImpls List of implementations of the file copy interface
     * @param clusterLoadBalancer  Implementation of the cluster load balancer interface
     * @param workflowExecutor An executor that executes impl in a workflow
     */
    @Autowired
    // TODO Abuse of DI?
    public LocalJobSubmitterImpl(
        final JobPersistenceService jps,
        final ClusterService clusterService,
        final CommandService commandService,
        final ApplicationService applicationService,
        final ClusterLoadBalancer clusterLoadBalancer,
        final List<FileCopyService> fileCopyServiceImpls,
       // final List<WorkflowTask> impl,
        final WorkflowExecutor workflowExecutor
    ) {

        this.jobPersistenceService = jps;
        this.clusterService = clusterService;
        this.commandService = commandService;
        this.applicationService = applicationService;
        this.clusterLoadBalancer = clusterLoadBalancer;
       // this.jobWorkflowTasks = impl;
        this.wfExecutor = workflowExecutor;
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
        log.debug("called with job request {}", jobRequest);

        // construct the job execution environment object for this job request
        JobExecutionEnvironment jee = null;

        try {
            jee = new JobExecutionEnvironment(
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

        // TODO need null check for jee here?
        // Job can be run as there is a valid cluster/command combination for it.
        final HashMap<String, Object> contextDetails = new HashMap<>();
        contextDetails.put(JOB_EXECUTION_ENV_KEY, jee);
        contextDetails.put("fc", fileCopyServiceImpls);
        final SimpleContext sc = new SimpleContext(contextDetails);

        if (this.wfExecutor.executeWorkflow(this.jobWorkflowTasks, sc)) {
            final JobExecution jobExecution = (JobExecution) sc.getAttribute(JOB_EXECUTION_DTO_KEY);

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

            this.jobPersistenceService.createJobExecution(jobExecution);
        } else {
            throw new GenieServerException("Could not start genie job");
        }
    }
}
