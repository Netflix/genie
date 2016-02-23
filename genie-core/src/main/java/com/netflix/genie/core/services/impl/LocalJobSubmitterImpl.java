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
import com.netflix.genie.core.jobs.workflow.WorkflowExecutor;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.GenieFileTransferService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSubmitterService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private static final String FILE_TRANSFER_SERVICE_KEY = "fts";

    private final JobPersistenceService jobPersistenceService;
    private final ClusterService clusterService;
    private final CommandService commandService;
    private final ApplicationService applicationService;
    private final ClusterLoadBalancer clusterLoadBalancer;
    private final List<WorkflowTask> jobWorkflowTasks;

    private String baseWorkingDirPath;
    private GenieFileTransferService fileTransferService;
    private final WorkflowExecutor wfExecutor;

    /**
     * Constructor create the object.
     *
     * @param jps                  Implementation of the job persistence service
     * @param clusterService       Implementation of cluster service interface
     * @param commandService       Implementation of command service interface
     * @param applicationService   Implementation of the application service interface
     * @param fts File Transfer service
     * @param clusterLoadBalancer  Implementation of the cluster load balancer interface
     * @param workflowTasks List of all the workflow tasks to be executed
     * @param genieWorkingDir Working directory for genie where it creates jobs directories
     * @param workflowExecutor An executor that executes impl in a workflow
     */
    @Autowired
    public LocalJobSubmitterImpl(
        final JobPersistenceService jps,
        final ClusterService clusterService,
        final CommandService commandService,
        final ApplicationService applicationService,
        final ClusterLoadBalancer clusterLoadBalancer,
        final GenieFileTransferService fts,
        final List<WorkflowTask> workflowTasks,
        @Value("${genie.jobs.dir.location:/mnt/tomcat/genie-jobs}")
        final String genieWorkingDir,
        final WorkflowExecutor workflowExecutor
    ) {

        this.jobPersistenceService = jps;
        this.clusterService = clusterService;
        this.commandService = commandService;
        this.applicationService = applicationService;
        this.clusterLoadBalancer = clusterLoadBalancer;
        this.jobWorkflowTasks = workflowTasks;
        this.baseWorkingDirPath = genieWorkingDir;
        this.wfExecutor = workflowExecutor;
        this.fileTransferService = fts;
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

        // Job can be run as there is a valid cluster/command combination for it.
        final Map<String, Object> context = new HashMap<>();

        context.put(JOB_EXECUTION_ENV_KEY, jee);
        context.put(FILE_TRANSFER_SERVICE_KEY, fileTransferService);

        if (this.wfExecutor.executeWorkflow(this.jobWorkflowTasks, context)) {
            final JobExecution jobExecution = (JobExecution) context.get(JOB_EXECUTION_DTO_KEY);

            // TODO Null check for jobExecution and maybe persist only for non-local mode where this is null?
            // Persist the jobExecution information. This also updates jobStatus to Running
            this.jobPersistenceService.createJobExecution(jobExecution);

            // Update the Cluster Information for the job
            this.jobPersistenceService.updateClusterForJob(
                jobRequest.getId(),
                jee.getCluster().getId());

            // Update the Command Information for the job
            this.jobPersistenceService.updateCommandForJob(
                jobRequest.getId(),
                jee.getCommand().getId());
        } else {
            throw new GenieServerException("Job Submission failed.");
        }
    }
}
