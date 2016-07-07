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
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
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
    private final GenieFileTransferService fileTransferService;

    /**
     * Constructor create the object.
     *
     * @param jobPersistenceService     Implementation of the job persistence service
     * @param applicationService        Implementation of application service interface
     * @param clusterService            Implementation of cluster service interface
     * @param commandService            Implementation of command service interface
     * @param clusterLoadBalancer       Implementation of the cluster load balancer interface
     * @param fileTransferService       File Transfer service
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
        @NotNull final GenieFileTransferService fileTransferService,
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
        this.fileTransferService = fileTransferService;
        this.applicationEventPublisher = applicationEventPublisher;
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
            final File jobWorkingDir;

            try {
                jobWorkingDir = new File(baseWorkingDirPath.getFile(), "/" + id);
            } catch (final IOException ioe) {
                throw new GenieServerException("Could not resolve job working directory due to exception", ioe);
            }

            // Resolve the cluster for the job request based on the tags specified
            //TODO: Combine the cluster and command selection into a single method/database query for efficiency
            final Cluster cluster;
            try {
                cluster =
                    this.clusterLoadBalancer.selectCluster(this.clusterService.chooseClusterForJobRequest(jobRequest));
            } catch (GeniePreconditionException gpe) {
                log.error(gpe.getLocalizedMessage(), gpe);
                this.jobPersistenceService.updateJobStatus(
                    id,
                    JobStatus.INVALID,
                    "No cluster found for tags specified.");
                throw gpe;
            }


            // Resolve the command for the job request based on command tags and cluster chosen
            final Set<CommandStatus> enumStatuses = EnumSet.noneOf(CommandStatus.class);
            enumStatuses.add(CommandStatus.ACTIVE);
            Command command = null;

            // TODO: what happens if the get method throws an error we don't mark the job failed here
            for (final Command cmd : this.clusterService.getCommandsForCluster(cluster.getId(), enumStatuses)) {
                if (cmd.getTags().containsAll(jobRequest.getCommandCriteria())) {
                    command = cmd;
                    break;
                }
            }

            if (command == null) {
                this.jobPersistenceService.updateJobStatus(
                    id,
                    JobStatus.INVALID,
                    "No command found for tags specified.");
                throw new GeniePreconditionException(
                    "No command found matching all command criteria on cluster. Unable to continue."
                );
            }

            // TODO: What do we do about application status? Should probably check here
            final List<Application> applications = new ArrayList<>();
            if (jobRequest.getApplications().isEmpty()) {
                applications.addAll(this.commandService.getApplicationsForCommand(command.getId()));
            } else {
                for (final String applicationId : jobRequest.getApplications()) {
                    applications.add(this.applicationService.getApplication(applicationId));
                }
            }

            // Job can be run as there is a valid set of cluster, command and applications
            // Save all the runtime environment information for the job
            this.jobPersistenceService.updateJobWithRuntimeEnvironment(
                id,
                cluster.getId(),
                command.getId(),
                applications.stream().map(Application::getId).collect(Collectors.toList())
            );

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
            context.put(JobConstants.FILE_TRANSFER_SERVICE_KEY, this.fileTransferService);

            final String runScript;
            try {
                // Create the job working directory
                final File dir = new File(jobWorkingDir.getCanonicalPath());
                if (!dir.mkdirs()) {
                    throw new GenieServerException("Could not create job working directory directory: "
                        + jobWorkingDir.getCanonicalPath());
                }

                // Run script path for this job like basedir/jobId/run.sh
                runScript = jobWorkingDir.getCanonicalPath()
                    + JobConstants.FILE_PATH_DELIMITER
                    + JobConstants.GENIE_JOB_LAUNCHER_SCRIPT;

            } catch (final IOException e) {
                throw new GenieServerException("Job submission failed.", e);
            }

            try (final Writer writer = new OutputStreamWriter(new FileOutputStream(runScript), "UTF-8")) {

                final File file = new File(runScript);
                file.setExecutable(true);

                context.put(JobConstants.WRITER_KEY, writer);

                for (WorkflowTask workflowTask : this.jobWorkflowTasks) {
                    workflowTask.executeTask(context);
                }
            } catch (final IOException ioe) {
                throw new GenieServerException("Failed to execute job", ioe);
            }

            final JobExecution jobExecution = (JobExecution) context.get(JobConstants.JOB_EXECUTION_DTO_KEY);

            // Job Execution will be null in local mode.
            if (jobExecution != null) {
                // Persist the jobExecution information. This also updates jobStatus to Running
                this.jobPersistenceService.createJobExecution(jobExecution);

                // Publish a job start Event
                this.applicationEventPublisher.publishEvent(new JobStartedEvent(jobExecution, this));
            }
        } catch (final Exception e) {
            log.error(e.getLocalizedMessage(), e);
            this.jobPersistenceService.updateJobStatus(id, JobStatus.FAILED, e.getLocalizedMessage());
            throw e;
        }
    }
}
