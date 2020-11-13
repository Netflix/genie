/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.web.agent.services.impl;

import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieAgentRejectedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieIdAlreadyExistsException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobResolutionRuntimeException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException;
import com.netflix.genie.web.agent.inspectors.InspectionReport;
import com.netflix.genie.web.agent.services.AgentConfigurationService;
import com.netflix.genie.web.agent.services.AgentFilterService;
import com.netflix.genie.web.agent.services.AgentJobService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.dtos.JobSubmission;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

/**
 * Default implementation of {@link AgentJobService}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
@Transactional
public class AgentJobServiceImpl implements AgentJobService {

    private static final String AGENT_JOB_SERVICE_METRIC_PREFIX = "genie.services.agentJob.";
    private static final String HANDSHAKE_COUNTER_METRIC_NAME = AGENT_JOB_SERVICE_METRIC_PREFIX + "handshake.counter";
    private static final String GET_AGENT_PROPERTIES_COUNTER_METRIC_NAME
        = AGENT_JOB_SERVICE_METRIC_PREFIX + "getAgentProperties.counter";
    private static final String AGENT_VERSION_METRIC_TAG_NAME = "agentVersion";
    private static final String HANDSHAKE_DECISION_METRIC_TAG_NAME = "handshakeDecision";
    private final PersistenceService persistenceService;
    private final JobResolverService jobResolverService;
    private final AgentFilterService agentFilterService;
    private final AgentConfigurationService agentConfigurationService;
    private final MeterRegistry meterRegistry;

    /**
     * Constructor.
     *
     * @param dataServices              The {@link DataServices} instance to use
     * @param jobResolverService        The specification service to use
     * @param agentFilterService        The agent filter service to use
     * @param agentConfigurationService The agent configuration service
     * @param meterRegistry             The metrics registry to use
     */
    public AgentJobServiceImpl(
        final DataServices dataServices,
        final JobResolverService jobResolverService,
        final AgentFilterService agentFilterService,
        final AgentConfigurationService agentConfigurationService, final MeterRegistry meterRegistry
    ) {
        this.persistenceService = dataServices.getPersistenceService();
        this.jobResolverService = jobResolverService;
        this.agentFilterService = agentFilterService;
        this.agentConfigurationService = agentConfigurationService;
        this.meterRegistry = meterRegistry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handshake(
        @Valid final AgentClientMetadata agentClientMetadata
    ) throws GenieAgentRejectedException {

        final HashSet<Tag> tags = Sets.newHashSet(
            Tag.of(AGENT_VERSION_METRIC_TAG_NAME, agentClientMetadata.getVersion().orElse("null"))
        );

        final InspectionReport report;
        try {
            report = agentFilterService.inspectAgentMetadata(agentClientMetadata);
        } catch (final Exception e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            meterRegistry.counter(HANDSHAKE_COUNTER_METRIC_NAME, tags).increment();
            throw e;
        }

        MetricsUtils.addSuccessTags(tags);
        tags.add(Tag.of(HANDSHAKE_DECISION_METRIC_TAG_NAME, report.getDecision().name()));
        meterRegistry.counter(HANDSHAKE_COUNTER_METRIC_NAME, tags).increment();

        if (report.getDecision() == InspectionReport.Decision.REJECT) {
            throw new GenieAgentRejectedException("Agent rejected: " + report.getMessage());
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAgentProperties(
        @Valid final AgentClientMetadata agentClientMetadata
    ) {
        final HashSet<Tag> tags = Sets.newHashSet(
            Tag.of(AGENT_VERSION_METRIC_TAG_NAME, agentClientMetadata.getVersion().orElse("null"))
        );

        try {
            final Map<String, String> agentPropertiesMap = this.agentConfigurationService.getAgentProperties();
            MetricsUtils.addSuccessTags(tags);
            return agentPropertiesMap;
        } catch (final Exception e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } finally {
            this.meterRegistry.counter(GET_AGENT_PROPERTIES_COUNTER_METRIC_NAME, tags).increment();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String reserveJobId(
        @Valid final JobRequest jobRequest,
        @Valid final AgentClientMetadata agentClientMetadata
    ) {
        final JobRequestMetadata jobRequestMetadata = new JobRequestMetadata(null, agentClientMetadata, 0, 0, null);
        try {
            return this.persistenceService.saveJobSubmission(
                new JobSubmission.Builder(jobRequest, jobRequestMetadata).build()
            );
        } catch (final IdAlreadyExistsException e) {
            // TODO: How to handle this?
            throw new GenieIdAlreadyExistsException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification resolveJobSpecification(
        @NotBlank final String id
    ) throws GenieJobResolutionException, GenieJobResolutionRuntimeException {
        try {
            final JobRequest jobRequest = this.persistenceService.getJobRequest(id);
            final ResolvedJob resolvedJob = this.jobResolverService.resolveJob(id, jobRequest, false);
            this.persistenceService.saveResolvedJob(id, resolvedJob);
            return resolvedJob.getJobSpecification();
        } catch (final NotFoundException e) {
            throw new GenieJobResolutionException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public JobSpecification getJobSpecification(@NotBlank final String id) {
        try {
            return this.persistenceService
                .getJobSpecification(id)
                .orElseThrow(
                    () -> new GenieJobSpecificationNotFoundException(
                        "No job specification exists for job with id " + id
                    )
                );
        } catch (final NotFoundException e) {
            throw new GenieJobNotFoundException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public JobSpecification dryRunJobSpecificationResolution(
        @Valid final JobRequest jobRequest
    ) throws GenieJobResolutionException {
        return this.jobResolverService.resolveJob(
            jobRequest.getRequestedId().orElse(UUID.randomUUID().toString()),
            jobRequest,
            false
        ).getJobSpecification();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void claimJob(@NotBlank final String id, @Valid final AgentClientMetadata agentClientMetadata) {
        try {
            this.persistenceService.claimJob(id, agentClientMetadata);
        } catch (final NotFoundException e) {
            throw new GenieJobNotFoundException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateJobStatus(
        @NotBlank final String id,
        final JobStatus currentStatus,
        final JobStatus newStatus,
        @Nullable final String newStatusMessage
    ) {
        try {
            this.persistenceService.updateJobStatus(id, currentStatus, newStatus, newStatusMessage);
        } catch (final NotFoundException e) {
            throw new GenieJobNotFoundException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatus getJobStatus(@NotBlank final String id) {
        try {
            return this.persistenceService.getJobStatus(id);
        } catch (final NotFoundException e) {
            throw new GenieJobNotFoundException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateJobArchiveStatus(@NotBlank final String id, final ArchiveStatus newArchiveStatus) {
        try {
            this.persistenceService.updateJobArchiveStatus(id, newArchiveStatus);
        } catch (NotFoundException e) {
            throw new GenieJobNotFoundException(e);
        }
    }
}
