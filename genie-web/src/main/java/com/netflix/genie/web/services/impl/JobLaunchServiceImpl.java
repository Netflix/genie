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

import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.dtos.JobSubmission;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.AgentLaunchException;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import com.netflix.genie.web.services.JobLaunchService;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.validation.Valid;
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

    private final JobPersistenceService jobPersistenceService;
    private final JobResolverService jobResolverService;
    private final AgentLauncher agentLauncher;
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param jobPersistenceService {@link JobPersistenceService} implementation to save job data
     * @param jobResolverService    {@link JobResolverService} implementation used to resolve job details
     * @param agentLauncher         {@link AgentLauncher} implementation to launch agents
     * @param registry              {@link MeterRegistry} metrics repository
     */
    public JobLaunchServiceImpl(
        final JobPersistenceService jobPersistenceService,
        final JobResolverService jobResolverService,
        final AgentLauncher agentLauncher,
        final MeterRegistry registry
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.jobResolverService = jobResolverService;
        this.agentLauncher = agentLauncher;
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public String launchJob(
        @Valid final JobSubmission jobSubmission
    ) throws AgentLaunchException, GenieJobResolutionException, IdAlreadyExistsException, SaveAttachmentException {
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

            final String jobId = this.jobPersistenceService.saveJobSubmission(jobSubmission);

            final ResolvedJob resolvedJob;
            try {
                resolvedJob = this.jobResolverService.resolveJob(jobId);
            } catch (final Throwable t) {
                MetricsUtils.addFailureTagsWithException(tags, t);
                this.jobPersistenceService.updateJobStatus(
                    jobId,
                    JobStatus.RESERVED,
                    JobStatus.FAILED,
                    t.getMessage()
                );
                throw t; // Caught below for metrics gathering
            }

            // Job state should be RESOLVED now. Mark it ACCEPTED to avoid race condition with agent starting up
            // before we get return from launchAgent and trying to set it to CLAIMED
            try {
                this.jobPersistenceService.updateJobStatus(
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

            // Already throws an exception
            try {
                this.agentLauncher.launchAgent(resolvedJob);
            } catch (final AgentLaunchException e) {
                // TODO: this could fail as well
                this.jobPersistenceService.updateJobStatus(jobId, JobStatus.ACCEPTED, JobStatus.FAILED, e.getMessage());
                // TODO: How will we get the ID back to the user? Should we add it to an exception? We don't get
                //       We don't get the ID until after saveJobSubmission so if that fails we'd still return nothing
                //       Probably need multiple exceptions to be thrown from this API (if we go with checked)
                throw e;
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
}
