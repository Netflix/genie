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

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.events.JobStartedEvent;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.jobs.workflow.WorkflowExecutor;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.GenieFileTransferService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSubmitterService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final ClusterLoadBalancer clusterLoadBalancer;
    private final List<WorkflowTask> jobWorkflowTasks;
    private final String baseWorkingDirPath;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final GenieFileTransferService fileTransferService;
    private final WorkflowExecutor wfExecutor;

    /**
     * Constructor create the object.
     *
     * @param jps                  Implementation of the job persistence service
     * @param clusterService       Implementation of cluster service interface
     * @param commandService       Implementation of command service interface
     * @param clusterLoadBalancer  Implementation of the cluster load balancer interface
     * @param fts File Transfer service
     * @param workflowExecutor An executor that executes impl in a workflow
     * @param aep Instance of the event publisher
     * @param workflowTasks List of all the workflow tasks to be executed
     * @param genieWorkingDir Working directory for genie where it creates jobs directories
     */
    @Autowired
    public LocalJobSubmitterImpl(
        final JobPersistenceService jps,
        final ClusterService clusterService,
        final CommandService commandService,
        final ClusterLoadBalancer clusterLoadBalancer,
        final GenieFileTransferService fts,
        final WorkflowExecutor workflowExecutor,
        final ApplicationEventPublisher aep,
        final List<WorkflowTask> workflowTasks,
        @Value("${genie.jobs.dir.location:${null}")
        final String genieWorkingDir
    ) {

        this.jobPersistenceService = jps;
        this.clusterService = clusterService;
        this.commandService = commandService;
        this.clusterLoadBalancer = clusterLoadBalancer;
        this.jobWorkflowTasks = workflowTasks;
        this.baseWorkingDirPath = genieWorkingDir;
        this.wfExecutor = workflowExecutor;
        this.fileTransferService = fts;
        this.applicationEventPublisher = aep;
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

        if (StringUtils.isBlank(this.baseWorkingDirPath)) {
            throw new GenieServerException("Genie jobs dir location not set.");
        }

        final String jobWorkingDir = baseWorkingDirPath + "/" + jobRequest.getId();

        // Resolve the cluster for the job request based on the tags specified
        final Cluster cluster;
        try {
            cluster = clusterLoadBalancer
                .selectCluster(clusterService.chooseClusterForJobRequest(jobRequest));
        } catch (GeniePreconditionException gpe) {
            this.jobPersistenceService.updateJobStatus(
                jobRequest.getId(),
                JobStatus.INVALID,
                "Unable to resolve to valid cluster/command combination for criteria specified.");
            throw gpe;
        }

        // Resolve the command for the job request based on command tags and cluster choosen
        final Set<CommandStatus> enumStatuses = EnumSet.noneOf(CommandStatus.class);
        enumStatuses.add(CommandStatus.ACTIVE);
        Command command = null;

        for (final Command cmd : this.clusterService.getCommandsForCluster(
            cluster.getId(),
            enumStatuses
        )) {
            if (cmd.getTags().containsAll(jobRequest.getCommandCriteria())) {
                command = cmd;
                break;
            }
        }

        if (command == null) {
            final String msg = "No command found for params. Unable to continue.";
            log.error(msg);
            throw new GeniePreconditionException(msg);
        }

        // Job can be run as there is a valid cluster/command combination for it.
        // Update cluster and command information for the job
        this.jobPersistenceService.updateClusterForJob(
            jobRequest.getId(),
            cluster.getId());

        this.jobPersistenceService.updateCommandForJob(
            jobRequest.getId(),
            command.getId());

        // construct the job execution environment object for this job request
        final JobExecutionEnvironment jee = new JobExecutionEnvironment.Builder(
            jobRequest,
            cluster,
            command,
            jobWorkingDir
        )
            .withApplications(commandService.getApplicationsForCommand(command.getId()))
            .build();

        // The map object stores the context for all the workflow tasks
        final Map<String, Object> context = new HashMap<>();

        context.put(JOB_EXECUTION_ENV_KEY, jee);
        context.put(FILE_TRANSFER_SERVICE_KEY, fileTransferService);

        if (this.wfExecutor.executeWorkflow(this.jobWorkflowTasks, context)) {
            final JobExecution jobExecution = (JobExecution) context.get(JOB_EXECUTION_DTO_KEY);

            // Job Execution will be null in local mode.
            if (jobExecution != null) {
                // Persist the jobExecution information. This also updates jobStatus to Running
                this.jobPersistenceService.createJobExecution(jobExecution);

                // Publish a job start Event
                this.applicationEventPublisher.publishEvent(new JobStartedEvent(jobExecution, this));
            }
        } else {
            throw new GenieServerException("Job Submission failed.");
        }
    }
}
