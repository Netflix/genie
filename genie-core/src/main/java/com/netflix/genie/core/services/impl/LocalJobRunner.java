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

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.core.events.JobFinishedReason;
import com.netflix.genie.core.events.JobStartedEvent;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSubmitterService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of the Job Submitter service that runs the job locally on the same host.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class LocalJobRunner implements JobSubmitterService {

    private final JobPersistenceService jobPersistenceService;
    private final ApplicationService applicationService;
    private final ClusterService clusterService;
    private final CommandService commandService;
    private final ClusterLoadBalancer clusterLoadBalancer;
    private final List<WorkflowTask> jobWorkflowTasks;
    private final Resource baseWorkingDirPath;
    private final ApplicationEventPublisher applicationEventPublisher;

    // For reuse in queries
    private final Set<CommandStatus> enumStatuses;

    /**
     * Constructor create the object.
     *
     * @param jobPersistenceService     Implementation of the job persistence service
     * @param applicationService        Implementation of application service interface
     * @param clusterService            Implementation of cluster service interface
     * @param commandService            Implementation of command service interface
     * @param clusterLoadBalancer       Implementation of the cluster load balancer interface
     * @param applicationEventPublisher Instance of the event publisher
     * @param workflowTasks             List of all the workflow tasks to be executed
     * @param genieWorkingDir           Working directory for genie where it creates jobs directories
     */
    public LocalJobRunner(
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final ApplicationService applicationService,
        @NotNull final ClusterService clusterService,
        @NotNull final CommandService commandService,
        @NotNull final ClusterLoadBalancer clusterLoadBalancer,
        @NotNull final ApplicationEventPublisher applicationEventPublisher,
        @NotNull final List<WorkflowTask> workflowTasks,
        @NotNull final Resource genieWorkingDir
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.applicationService = applicationService;
        this.clusterService = clusterService;
        this.commandService = commandService;
        this.clusterLoadBalancer = clusterLoadBalancer;
        this.jobWorkflowTasks = workflowTasks;
        this.baseWorkingDirPath = genieWorkingDir;
        this.applicationEventPublisher = applicationEventPublisher;

        // We'll only care about active statuses
        this.enumStatuses = EnumSet.noneOf(CommandStatus.class);
        this.enumStatuses.add(CommandStatus.ACTIVE);
    }

    /**
     * Submit the job for appropriate execution based on environment.
     *
     * @param jobRequest of job to run
     * @throws GenieException if there is an error
     */
    @SuppressFBWarnings(
        value = "REC_CATCH_EXCEPTION",
        justification = "We catch exception to make sure we always mark job failed."
    )
    @Override
    public void submitJob(
        @NotNull(message = "No job provided. Unable to submit job for execution.")
        @Valid
        final JobRequest jobRequest
    ) throws GenieException {
        log.debug("called with job request {}", jobRequest);
        final String id = jobRequest.getId();

        try {
            final File jobWorkingDir = this.createJobWorkingDirectory(id);
            final File runScript = this.createRunScript(jobWorkingDir);

            //TODO: Combine the cluster and command selection into a single method/database query for efficiency
            // Resolve the cluster for the job request based on the tags specified
            final Cluster cluster = this.getCluster(jobRequest);
            // Resolve the command for the job request based on command tags and cluster chosen
            final Command command = this.getCommand(jobRequest, cluster);
            // Resolve the applications to use based on the command that was selected
            final List<Application> applications = this.getApplications(jobRequest, command);

            // Job can be run as there is a valid set of cluster, command and applications
            // Save all the runtime environment information for the job
            this.jobPersistenceService.updateJobWithRuntimeEnvironment(
                id,
                cluster.getId(),
                command.getId(),
                applications.stream().map(Application::getId).collect(Collectors.toList())
            );

            // The map object stores the context for all the workflow tasks
            final Map<String, Object> context
                = this.createJobContext(jobRequest, cluster, command, applications, jobWorkingDir);

            // Execute the job
            final JobExecution jobExecution = this.executeJob(context, runScript);

            // Job Execution will be null in local mode.
            if (jobExecution != null) {
                // Persist the jobExecution information. This also updates jobStatus to Running
                this.jobPersistenceService.createJobExecution(jobExecution);

                // Publish a job start Event
                this.applicationEventPublisher.publishEvent(new JobStartedEvent(jobExecution, this));
            }
        } catch (final GeniePreconditionException gpe) {
            log.error(gpe.getMessage(), gpe);
            this.applicationEventPublisher.publishEvent(
                new JobFinishedEvent(id, JobFinishedReason.INVALID, gpe.getMessage(), this)
            );
            throw gpe;
        } catch (final Exception e) {
            log.error(e.getMessage(), e);
            this.applicationEventPublisher.publishEvent(
                new JobFinishedEvent(id, JobFinishedReason.FAILED_TO_INIT, e.getMessage(), this)
            );
            throw e;
        }
    }

    private File createJobWorkingDirectory(final String id) throws GenieException {
        try {
            final File jobDir = new File(this.baseWorkingDirPath.getFile(), id);
            if (!jobDir.mkdirs()) {
                throw new GenieServerException(
                    "Could not create job working directory directory: " + jobDir.getCanonicalPath()
                );
            }
            return jobDir;
        } catch (final IOException ioe) {
            throw new GenieServerException("Could not resolve job working directory due to exception", ioe);
        }
    }

    private File createRunScript(final File jobWorkingDir) throws GenieException {
        final File runScript = new File(jobWorkingDir, JobConstants.GENIE_JOB_LAUNCHER_SCRIPT);
        if (!runScript.exists()) {
            try {
                if (!runScript.createNewFile()) {
                    throw new GenieServerException("Unable to create run script file due to unknown reason.");
                }
            } catch (final IOException ioe) {
                throw new GenieServerException("Unable to create run script file due to IOException.", ioe);
            }
        }
        if (!runScript.setExecutable(true)) {
            throw new GenieServerException("Unable to make run script executable");
        }
        return runScript;
    }

    private Cluster getCluster(final JobRequest jobRequest) throws GenieException {
        return this.clusterLoadBalancer.selectCluster(this.clusterService.chooseClusterForJobRequest(jobRequest));
    }

    private Command getCommand(final JobRequest jobRequest, final Cluster cluster) throws GenieException {
        final Set<String> commandCriteria = jobRequest.getCommandCriteria();
        // TODO: what happens if the get method throws an error we don't mark the job failed here
        for (final Command command : this.clusterService.getCommandsForCluster(cluster.getId(), this.enumStatuses)) {
            if (command.getTags().containsAll(jobRequest.getCommandCriteria())) {
                return command;
            }
        }

        throw new GeniePreconditionException(
            "No command found matching all command criteria ["
                + commandCriteria
                + "] attached to cluster with id: "
                + cluster.getId()
        );
    }

    private List<Application> getApplications(
        final JobRequest jobRequest,
        final Command command
    ) throws GenieException {
        // TODO: What do we do about application status? Should probably check here
        final List<Application> applications = new ArrayList<>();
        if (jobRequest.getApplications().isEmpty()) {
            applications.addAll(this.commandService.getApplicationsForCommand(command.getId()));
        } else {
            for (final String applicationId : jobRequest.getApplications()) {
                applications.add(this.applicationService.getApplication(applicationId));
            }
        }
        return applications;
    }

    private Map<String, Object> createJobContext(
        final JobRequest jobRequest,
        final Cluster cluster,
        final Command command,
        final List<Application> applications,
        final File jobWorkingDir
    ) throws GenieException {
        // construct the job execution environment object for this job request
        final JobExecutionEnvironment jee = new JobExecutionEnvironment.Builder(
            jobRequest,
            cluster,
            command,
            jobWorkingDir
        )
            .withApplications(applications)
            .build();

        // The map object stores the context for all the workflow tasks
        final Map<String, Object> context = new HashMap<>();

        context.put(JobConstants.JOB_EXECUTION_ENV_KEY, jee);

        return context;
    }

    private JobExecution executeJob(final Map<String, Object> context, final File runScript) throws GenieException {
        try (final Writer writer = new OutputStreamWriter(new FileOutputStream(runScript), "UTF-8")) {
            context.put(JobConstants.WRITER_KEY, writer);

            for (WorkflowTask workflowTask : this.jobWorkflowTasks) {
                workflowTask.executeTask(context);
            }
        } catch (final IOException ioe) {
            throw new GenieServerException("Failed to execute job due to: " + ioe.getMessage(), ioe);
        }

        return (JobExecution) context.get(JobConstants.JOB_EXECUTION_DTO_KEY);
    }
}
