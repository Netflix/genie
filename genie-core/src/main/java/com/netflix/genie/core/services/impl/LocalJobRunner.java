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
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.core.events.JobStartedEvent;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.core.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.Resource;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
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
@Slf4j
public class LocalJobRunner implements JobSubmitterService {

    private final JobSearchService jobSearchService;
    private final JobPersistenceService jobPersistenceService;
    private final ClusterService clusterService;
    private final CommandService commandService;
    private final ClusterLoadBalancer clusterLoadBalancer;
    private final List<WorkflowTask> jobWorkflowTasks;
    private final Resource baseWorkingDirPath;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final GenieFileTransferService fileTransferService;
    private final String hostname;
    private final int maxRunningJobs;

    /**
     * Constructor create the object.
     *
     * @param jss                  Implementaion of the jobSearchService
     * @param jps                  Implementation of the job persistence service
     * @param clusterService       Implementation of cluster service interface
     * @param commandService       Implementation of command service interface
     * @param clusterLoadBalancer  Implementation of the cluster load balancer interface
     * @param fts File Transfer service
     * @param aep Instance of the event publisher
     * @param workflowTasks List of all the workflow tasks to be executed
     * @param genieWorkingDir Working directory for genie where it creates jobs directories
     * @param hostname Hostname of this host
     * @param maxRunningJobs Maximum number of jobs allowed to run on this host
     */
    public LocalJobRunner(
        final JobSearchService jss,
        final JobPersistenceService jps,
        final ClusterService clusterService,
        final CommandService commandService,
        final ClusterLoadBalancer clusterLoadBalancer,
        final GenieFileTransferService fts,
        final ApplicationEventPublisher aep,
        final List<WorkflowTask> workflowTasks,
        final Resource genieWorkingDir,
        final String hostname,
        final int maxRunningJobs
    ) {
        this.jobSearchService = jss;
        this.jobPersistenceService = jps;
        this.clusterService = clusterService;
        this.commandService = commandService;
        this.clusterLoadBalancer = clusterLoadBalancer;
        this.jobWorkflowTasks = workflowTasks;
        this.baseWorkingDirPath = genieWorkingDir;
        this.fileTransferService = fts;
        this.applicationEventPublisher = aep;
        this.hostname = hostname;
        this.maxRunningJobs = maxRunningJobs;
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

        if (this.jobSearchService.getAllRunningJobExecutionsOnHost(this.hostname).size() > this.maxRunningJobs) {
            throw new GenieServerUnavailableException("Reached max running jobs on this host. Rejecting request");
        }

        final File jobWorkingDir;

        try {
            jobWorkingDir = new File(baseWorkingDirPath.getFile(), "/" + jobRequest.getId());
        } catch (IOException ioe) {
            throw new GenieServerException("Could not resolve job working directory due to exception" + ioe);
        }

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

        context.put(JobConstants.JOB_EXECUTION_ENV_KEY, jee);
        context.put(JobConstants.FILE_TRANSFER_SERVICE_KEY, fileTransferService);

        final String runScript;
        try {
            // Create the job working directory
            Utils.createDirectory(jobWorkingDir.getCanonicalPath());

            // Run script path for this job like basedir/jobuuid/run.sh
            runScript = jobWorkingDir.getCanonicalPath()
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.GENIE_JOB_LAUNCHER_SCRIPT;

        } catch (IOException e) {
            throw new GenieServerException("Job submission failed.", e);
        }

        try (final Writer writer = new OutputStreamWriter(new FileOutputStream(runScript), "UTF-8")) {
            context.put(JobConstants.WRITER_KEY, writer);

            for (WorkflowTask workflowTask : this.jobWorkflowTasks) {
                workflowTask.executeTask(context);
            }
        } catch (IOException ioe) {
            throw new GenieServerException("Failed to execute job");
        }

        final JobExecution jobExecution = (JobExecution) context.get(JobConstants.JOB_EXECUTION_DTO_KEY);

        // Job Execution will be null in local mode.
        if (jobExecution != null) {
            // Persist the jobExecution information. This also updates jobStatus to Running
            this.jobPersistenceService.createJobExecution(jobExecution);

            // Publish a job start Event
            this.applicationEventPublisher.publishEvent(new JobStartedEvent(jobExecution, this));
        }
    }
}
