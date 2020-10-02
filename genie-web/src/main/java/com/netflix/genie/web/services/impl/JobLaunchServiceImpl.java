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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
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

    private static final String LAUNCH_JOB_TIMER = "genie.services.jobLaunch.launchJob.timer";
    private static final String AGENT_LAUNCHER_SELECTOR_TIMER = "genie.services.jobLaunch.selectLauncher.timer";
    private static final String AVAILABLE_LAUNCHERS_TAG = "numAvailableLaunchers";
    private static final String SELECTOR_CLASS_TAG = "agentLauncherSelectorClass";
    private static final String LAUNCHER_CLASS_TAG = "agentLauncherSelectedClass";

    private final PersistenceService persistenceService;
    private final JobResolverService jobResolverService;
    private final AgentLauncherSelector agentLauncherSelector;
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param dataServices          The {@link DataServices} instance to use
     * @param jobResolverService    {@link JobResolverService} implementation used to resolve job details
     * @param agentLauncherSelector {@link AgentLauncher} implementation to launch agents
     * @param registry              {@link MeterRegistry} metrics repository
     */
    public JobLaunchServiceImpl(
        final DataServices dataServices,
        final JobResolverService jobResolverService,
        final AgentLauncherSelector agentLauncherSelector,
        final MeterRegistry registry
    ) {
        this.persistenceService = dataServices.getPersistenceService();
        this.jobResolverService = jobResolverService;
        this.agentLauncherSelector = agentLauncherSelector;
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

            final ResolvedJob resolvedJob;
            try {
                resolvedJob = this.jobResolverService.resolveJob(jobId);
            } catch (final Throwable t) {
                MetricsUtils.addFailureTagsWithException(tags, t);
                this.persistenceService.updateJobStatus(
                    jobId,
                    JobStatus.RESERVED,
                    JobStatus.FAILED,
                    JobStatusMessages.FAILED_TO_RESOLVE_JOB // TODO: Move somewhere not in genie-common
                );
                this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES);
                throw t; // Caught below for metrics gathering
            }

            // Job state should be RESOLVED now. Mark it ACCEPTED to avoid race condition with agent starting up
            // before we get return from launchAgent and trying to set it to CLAIMED
            try {
                this.persistenceService.updateJobStatus(
                    jobId,
                    JobStatus.RESOLVED,
                    JobStatus.ACCEPTED,
                    "The job has been accepted by the system for execution"
                );
            } catch (final Throwable t) {
                // TODO: Failed to update the status to accepted. Try to set it to failed or rely on other cleanup
                //       mechanism?
                throw new AgentLaunchException(t);
            }

            // TODO: at the moment this is not populated, it's going to be a null node (not null)
            final JsonNode requestedLauncherExt =
                this.persistenceService.getRequestedLauncherExt(jobId);

            final Optional<JsonNode> launcherExt;
            try {
                 launcherExt = this.selectLauncher(jobId, jobSubmission, resolvedJob)
                    .launchAgent(resolvedJob, requestedLauncherExt);
            } catch (final AgentLaunchException e) {
                // TODO: this could fail as well
                this.persistenceService.updateJobStatus(jobId, JobStatus.ACCEPTED, JobStatus.FAILED, e.getMessage());
                this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES);
                // TODO: How will we get the ID back to the user? Should we add it to an exception? We don't get
                //       We don't get the ID until after saveJobSubmission so if that fails we'd still return nothing
                //       Probably need multiple exceptions to be thrown from this API (if we go with checked)
                throw e;
            }

            if (launcherExt.isPresent()) {
                this.persistenceService.updateLauncherExt(jobId, launcherExt.get());
            }

            MetricsUtils.addSuccessTags(tags);
            return jobId;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
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
            return selectedLauncher;
        } catch (ResourceSelectionException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new AgentLaunchException("Failed to select an Agent Launcher", e);
        } finally {
            this.registry
                .timer(AGENT_LAUNCHER_SELECTOR_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }
}
