/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.services.impl;

import brave.SpanCustomizer;
import brave.Tracer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.dtos.JobSubmission;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.AgentLaunchException;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.selectors.AgentLauncherSelectionContext;
import com.netflix.genie.web.selectors.AgentLauncherSelector;
import com.netflix.genie.web.services.JobLaunchService;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.validation.Valid;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of the {@link JobLaunchService}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class JobLaunchServiceImpl implements JobLaunchService {
    static final String BEGIN_LAUNCH_JOB_ANNOTATION = "Beginning to Launch Job";
    static final String SAVED_JOB_SUBMISSION_ANNOTATION = "Saved Job Submission";
    static final String RESOLVED_JOB_ANNOTATION = "Resolved Job";
    static final String MARKED_JOB_ACCEPTED_ANNOTATION = "Marked Job Accepted";
    static final String LAUNCHED_AGENT_ANNOTATION = "Launched Agent";
    static final String SAVED_LAUNCHER_EXT_ANNOTATION = "Saved Launcher Ext Data";
    static final String END_LAUNCH_JOB_ANNOTATION = "Completed Launching Job";

    private static final String LAUNCH_JOB_TIMER = "genie.services.jobLaunch.launchJob.timer";
    private static final String AGENT_LAUNCHER_SELECTOR_TIMER = "genie.services.jobLaunch.selectLauncher.timer";
    private static final String AVAILABLE_LAUNCHERS_TAG = "numAvailableLaunchers";
    private static final String SELECTOR_CLASS_TAG = "agentLauncherSelectorClass";
    private static final String LAUNCHER_CLASS_TAG = "agentLauncherSelectedClass";
    private static final int MAX_STATUS_UPDATE_ATTEMPTS = 5;
    private static final int INITIAL_ATTEMPT = 0;
    private static final String ACCEPTED_MESSAGE = "The job has been accepted by the system for execution";

    private final PersistenceService persistenceService;
    private final JobResolverService jobResolverService;
    private final AgentLauncherSelector agentLauncherSelector;
    private final Tracer tracer;
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param dataServices          The {@link DataServices} instance to use
     * @param jobResolverService    {@link JobResolverService} implementation used to resolve job details
     * @param agentLauncherSelector {@link AgentLauncher} implementation to launch agents
     * @param tracingComponents     {@link BraveTracingComponents} instance to use to get access to instrumentation
     * @param registry              {@link MeterRegistry} metrics repository
     */
    public JobLaunchServiceImpl(
        final DataServices dataServices,
        final JobResolverService jobResolverService,
        final AgentLauncherSelector agentLauncherSelector,
        final BraveTracingComponents tracingComponents,
        final MeterRegistry registry
    ) {
        this.persistenceService = dataServices.getPersistenceService();
        this.jobResolverService = jobResolverService;
        this.agentLauncherSelector = agentLauncherSelector;
        this.tracer = tracingComponents.getTracer();
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public String launchJob(
        @Valid final JobSubmission jobSubmission
    ) throws
        AgentLaunchException,
        GenieJobResolutionException,
        IdAlreadyExistsException,
        NotFoundException {
        final long start = System.nanoTime();
        final SpanCustomizer span = this.tracer.currentSpanCustomizer();
        span.annotate(BEGIN_LAUNCH_JOB_ANNOTATION);
        final Set<Tag> tags = Sets.newHashSet();
        try {
            /*
             * Steps:
             *
             * 1. Save the job information
             * 2. Attempt to resolve the job information (includes saving)
             * 3. Mark the job as accepted
             * 4. Launch the agent process given the implementation configured for this Genie instance
             * 5. If the agent launch fails mark the job failed else return
             */
            final String jobId = this.persistenceService.saveJobSubmission(jobSubmission);
            span.annotate(SAVED_JOB_SUBMISSION_ANNOTATION);

            final ResolvedJob resolvedJob;
            try {
                resolvedJob = this.jobResolverService.resolveJob(jobId);
            } catch (final Throwable t) {
                final String message;
                if (t instanceof GenieJobResolutionException) {
                    message = JobStatusMessages.FAILED_TO_RESOLVE_JOB;
                } else {
                    message = JobStatusMessages.RESOLUTION_RUNTIME_ERROR;
                }

                MetricsUtils.addFailureTagsWithException(tags, t);
                this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES);
                if (this.updateJobStatus(jobId, JobStatus.FAILED, message, INITIAL_ATTEMPT) != JobStatus.FAILED) {
                    log.error("Updating status to failed didn't succeed");
                }
                throw t; // Caught below for metrics gathering
            }
            span.annotate(RESOLVED_JOB_ANNOTATION);

            // Job state should be RESOLVED now. Mark it ACCEPTED to avoid race condition with agent starting up
            // before we get return from launchAgent and trying to set it to CLAIMED
            try {
                final JobStatus updatedStatus = this.updateJobStatus(
                    jobId,
                    JobStatus.ACCEPTED,
                    ACCEPTED_MESSAGE,
                    INITIAL_ATTEMPT
                );
                if (updatedStatus != JobStatus.ACCEPTED) {
                    throw new AgentLaunchException("Unable to mark job accepted. Job state " + updatedStatus);
                }
            } catch (final Exception e) {
                this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES);
                // TODO: Failed to update the status to accepted. Try to set it to failed or rely on other cleanup
                //       mechanism? For now rely on janitor mechanisms
                throw e;
            }
            span.annotate(MARKED_JOB_ACCEPTED_ANNOTATION);

            // TODO: at the moment this is not populated, it's going to be a null node (not null)
            final JsonNode requestedLauncherExt = this.persistenceService.getRequestedLauncherExt(jobId);

            final Optional<JsonNode> launcherExt;
            try {
                final AgentLauncher launcher = this.selectLauncher(jobId, jobSubmission, resolvedJob);
                tags.add(Tag.of(LAUNCHER_CLASS_TAG, launcher.getClass().getCanonicalName()));
                launcherExt = launcher.launchAgent(resolvedJob, requestedLauncherExt);
            } catch (final AgentLaunchException e) {
                this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES);
                this.updateJobStatus(jobId, JobStatus.FAILED, e.getMessage(), INITIAL_ATTEMPT);
                // TODO: How will we get the ID back to the user? Should we add it to an exception? We don't get
                //       We don't get the ID until after saveJobSubmission so if that fails we'd still return nothing
                //       Probably need multiple exceptions to be thrown from this API (if we go with checked)
                throw e;
            }
            span.annotate(LAUNCHED_AGENT_ANNOTATION);

            if (launcherExt.isPresent()) {
                try {
                    this.persistenceService.updateLauncherExt(jobId, launcherExt.get());
                } catch (final Exception e) {
                    // Being unable to update the launcher ext is not optimal however
                    // it's not worth returning an error to the user at this point as
                    // the agent has launched and we have all the other pieces in place
                    log.error("Unable to update the launcher ext for job {}", jobId, e);
                }
            }
            span.annotate(SAVED_LAUNCHER_EXT_ANNOTATION);

            MetricsUtils.addSuccessTags(tags);
            return jobId;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            span.annotate(END_LAUNCH_JOB_ANNOTATION);
            this.registry
                .timer(LAUNCH_JOB_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private AgentLauncher selectLauncher(
        final String jobId,
        final JobSubmission jobSubmission,
        final ResolvedJob resolvedJob
    ) throws AgentLaunchException {
        final Collection<AgentLauncher> availableLaunchers = this.agentLauncherSelector.getAgentLaunchers();
        log.debug("Selecting agent launcher for job {} ({} available)", jobId, availableLaunchers.size());
        final AgentLauncherSelectionContext context = new AgentLauncherSelectionContext(
            jobId,
            jobSubmission.getJobRequest(),
            jobSubmission.getJobRequestMetadata(),
            resolvedJob,
            availableLaunchers
        );

        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        tags.add(Tag.of(AVAILABLE_LAUNCHERS_TAG, String.valueOf(availableLaunchers.size())));
        tags.add(Tag.of(SELECTOR_CLASS_TAG, this.agentLauncherSelector.getClass().getSimpleName()));

        final ResourceSelectionResult<AgentLauncher> selectionResult;
        try {
            selectionResult = this.agentLauncherSelector.select(context);
            final AgentLauncher selectedLauncher = selectionResult.getSelectedResource().orElseThrow(
                () -> new ResourceSelectionException(
                    "No AgentLauncher selected: "
                        + selectionResult.getSelectionRationale().orElse("Rationale unknown")
                )
            );
            MetricsUtils.addSuccessTags(tags);
            tags.add(Tag.of(LAUNCHER_CLASS_TAG, selectedLauncher.getClass().getSimpleName()));
            log.debug("Selected launcher {} for job {}", selectedLauncher, jobId);
            return selectedLauncher;
        } catch (ResourceSelectionException e) {
            log.error("Error selecting agent launcher", e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new AgentLaunchException("Failed to select an Agent Launcher", e);
        } finally {
            this.registry
                .timer(AGENT_LAUNCHER_SELECTOR_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Helper method to update the job status ONLY IF the job is not in a final state already. This will maintain the
     * final state that may have been set elsewhere by another process (e.g. kill request).
     *
     * @param jobId                The id of the job to update status for
     * @param desiredStatus        The {@link JobStatus} that is the status the job should be in the database after
     *                             exiting this method
     * @param desiredStatusMessage The status message for the database
     * @param attemptNumber        The number of attempts that have been made to update this job status
     * @return The {@link JobStatus} in the database after the final attempt of this method
     */
    private JobStatus updateJobStatus(
        final String jobId,
        final JobStatus desiredStatus,
        final String desiredStatusMessage,
        final int attemptNumber
    ) throws NotFoundException {
        final int nextAttemptNumber = attemptNumber + 1;
        final JobStatus currentStatus = this.persistenceService.getJobStatus(jobId);
        if (currentStatus.isFinished()) {
            log.info(
                "Won't change job status of {} from {} to {} desired status as {} is already a final status",
                jobId,
                currentStatus,
                desiredStatus,
                currentStatus
            );
            return currentStatus;
        } else {
            try {
                this.persistenceService.updateJobStatus(jobId, currentStatus, desiredStatus, desiredStatusMessage);
                return desiredStatus;
            } catch (final GenieInvalidStatusException e) {
                log.error(
                    "Job {} status changed from expected {}. Couldn't update to {}. Attempt {}",
                    jobId,
                    currentStatus,
                    desiredStatus,
                    nextAttemptNumber
                );
                // Recursive call that should break out if update is successful or job is now in a final state
                // or if attempts reach the max attempts
                if (nextAttemptNumber < MAX_STATUS_UPDATE_ATTEMPTS) {
                    return this.updateJobStatus(jobId, desiredStatus, desiredStatusMessage, attemptNumber + 1);
                } else {
                    // breakout condition, stop attempting to update DB
                    log.error(
                        "Out of attempts to update job {} status to {}. Unable to complete status update",
                        jobId,
                        desiredStatus,
                        e
                    );
                    return currentStatus;
                }
            }
        }
    }
}
