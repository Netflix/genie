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
package com.netflix.genie.web.events;

import com.amazonaws.services.sns.AmazonSNS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.dtos.v4.FinishedJob;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.properties.SNSNotificationsProperties;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;

import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * Publishes Amazon SNS notifications with detailed information about each completed job.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class JobFinishedSNSPublisher
    extends AbstractSNSPublisher
    implements ApplicationListener<JobStateChangeEvent> {

    private static final char COLON = ':';
    private static final String JOB_ID_KEY_NAME = "jobId";
    private static final String JOB_VERSION_KEY_NAME = "jobVersion";
    private static final String JOB_NAME_KEY_NAME = "jobName";
    private static final String JOB_USER_KEY_NAME = "jobUser";
    private static final String JOB_DESCRIPTION_KEY_NAME = "jobDescription";
    private static final String JOB_METADATA_KEY_NAME = "jobMetadata";
    private static final String JOB_TAGS_KEY_NAME = "jobTags";
    private static final String JOB_CREATED_TIMESTAMP_KEY_NAME = "jobCreatedTimestamp";
    private static final String JOB_CREATED_ISO_TIMESTAMP_KEY_NAME = "jobCreatedIsoTimestamp";
    private static final String JOB_STATUS_KEY_NAME = "jobStatus";
    private static final String JOB_COMMAND_CRITERION_KEY_NAME = "jobCommandCriterion";
    private static final String JOB_CLUSTER_CRITERIA_KEY_NAME = "jobClusterCriteria";
    private static final String JOB_STARTED_TIMESTAMP_KEY_NAME = "jobStartedTimestamp";
    private static final String JOB_STARTED_ISO_TIMESTAMP_KEY_NAME = "jobStartedIsoTimestamp";
    private static final String JOB_GROUPING_KEY_NAME = "jobGrouping";
    private static final String JOB_FINISHED_TIMESTAMP_KEY_NAME = "jobFinishedTimestamp";
    private static final String JOB_FINISHED_ISO_TIMESTAMP_KEY_NAME = "jobFinishedIsoTimestamp";
    private static final String JOB_AGENT_VERSION_KEY_NAME = "jobAgentVersion";
    private static final String JOB_GROUPING_INSTANCE_KEY_NAME = "jobGroupingInstance";
    private static final String JOB_STATUS_MESSAGE_KEY_NAME = "jobStatusMessage";
    private static final String JOB_API_CLIENT_HOSTNAME_KEY_NAME = "jobApiClientHostname";
    private static final String JOB_REQUESTED_MEMORY_KEY_NAME = "jobRequestedMemory";
    private static final String JOB_AGENT_HOSTNAME_KEY_NAME = "jobAgentHostname";
    private static final String JOB_API_CLIENT_USER_AGENT_KEY_NAME = "jobApiClientUserAgent";
    private static final String JOB_EXIT_CODE_KEY_NAME = "jobExitCode";
    private static final String JOB_NUM_ATTACHMENTS_KEY_NAME = "jobNumAttachments";
    private static final String JOB_ARCHIVE_LOCATION_KEY_NAME = "jobArchiveLocation";
    private static final String JOB_USED_MEMORY_KEY_NAME = "jobUsedMemory";
    private static final String JOB_ARGUMENTS_KEY_NAME = "jobArguments";
    private static final String COMMAND_ID_KEY_NAME = "commandId";
    private static final String COMMAND_NAME_KEY_NAME = "commandName";
    private static final String COMMAND_VERSION_KEY_NAME = "commandVersion";
    private static final String COMMAND_DESCRIPTION_KEY_NAME = "commandDescription";
    private static final String COMMAND_CREATED_TIMESTAMP_KEY_NAME = "commandCreatedTimestamp";
    private static final String COMMAND_CREATED_ISO_TIMESTAMP_KEY_NAME = "commandCreatedIsoTimestamp";
    private static final String COMMAND_UPDATED_TIMESTAMP_KEY_NAME = "commandUpdatedTimestamp";
    private static final String COMMAND_UPDATED_ISO_TIMESTAMP_KEY_NAME = "commandUpdatedIsoTimestamp";
    private static final String COMMAND_EXECUTABLE_KEY_NAME = "commandExecutable";
    private static final String CLUSTER_ID_KEY_NAME = "clusterId";
    private static final String CLUSTER_NAME_KEY_NAME = "clusterName";
    private static final String CLUSTER_VERSION_KEY_NAME = "clusterVersion";
    private static final String CLUSTER_DESCRIPTION_KEY_NAME = "clusterDescription";
    private static final String CLUSTER_CREATED_TIMESTAMP_KEY_NAME = "clusterCreatedTimestamp";
    private static final String CLUSTER_CREATED_ISO_TIMESTAMP_KEY_NAME = "clusterCreatedIsoTimestamp";
    private static final String CLUSTER_UPDATED_TIMESTAMP_KEY_NAME = "clusterUpdatedTimestamp";
    private static final String CLUSTER_UPDATED_ISO_TIMESTAMP_KEY_NAME = "clusterUpdatedIsoTimestamp";
    private static final String APPLICATIONS_KEY_NAME = "applications";

    private final PersistenceService persistenceService;

    /**
     * Constructor.
     *
     * @param snsClient    Amazon SNS client
     * @param properties   configuration properties
     * @param dataServices the {@link DataServices} instance to use
     * @param registry     metrics registry
     * @param mapper       object mapper
     */
    public JobFinishedSNSPublisher(
        final AmazonSNS snsClient,
        final SNSNotificationsProperties properties,
        final DataServices dataServices,
        final MeterRegistry registry,
        final ObjectMapper mapper
    ) {
        super(properties, registry, snsClient, mapper);
        this.persistenceService = dataServices.getPersistenceService();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onApplicationEvent(final JobStateChangeEvent event) {
        if (!this.properties.isEnabled()) {
            // Publishing is disabled
            return;
        }

        final JobStatus jobStatus = event.getNewStatus();
        if (!jobStatus.isFinished()) {
            // Job still in progress.
            return;
        }

        final String jobId = event.getJobId();

        final FinishedJob job;
        try {
            job = persistenceService.getFinishedJob(jobId);
        } catch (final NotFoundException | GenieInvalidStatusException e) {
            log.error("Failed to retrieve finished job: {}", jobId, e);
            return;
        }

        log.debug("Publishing SNS notification for completed job {}", jobId);

        final HashMap<String, Object> eventDetailsMap = Maps.newHashMap();

        eventDetailsMap.put(JOB_ID_KEY_NAME, jobId);
        eventDetailsMap.put(JOB_NAME_KEY_NAME, job.getName());
        eventDetailsMap.put(JOB_USER_KEY_NAME, job.getUser());
        eventDetailsMap.put(JOB_VERSION_KEY_NAME, job.getVersion());
        eventDetailsMap.put(JOB_DESCRIPTION_KEY_NAME, job.getDescription().orElse(null));
        eventDetailsMap.put(JOB_METADATA_KEY_NAME, job.getMetadata().orElse(null));
        eventDetailsMap.put(JOB_TAGS_KEY_NAME, job.getTags());
        eventDetailsMap.put(JOB_CREATED_TIMESTAMP_KEY_NAME, job.getCreated().toEpochMilli());
        eventDetailsMap.put(JOB_CREATED_ISO_TIMESTAMP_KEY_NAME, job.getCreated());
        eventDetailsMap.put(JOB_STATUS_KEY_NAME, job.getStatus());
        eventDetailsMap.put(JOB_COMMAND_CRITERION_KEY_NAME, job.getCommandCriterion());
        eventDetailsMap.put(JOB_CLUSTER_CRITERIA_KEY_NAME, job.getClusterCriteria());
        eventDetailsMap.put(
            JOB_STARTED_TIMESTAMP_KEY_NAME,
            job.getStarted().isPresent()
                ? job.getStarted().get().toEpochMilli()
                : null
        );
        eventDetailsMap.put(JOB_STARTED_ISO_TIMESTAMP_KEY_NAME, job.getStarted().orElse(null));
        eventDetailsMap.put(
            JOB_FINISHED_TIMESTAMP_KEY_NAME,
            job.getFinished().isPresent()
                ? job.getFinished().get().toEpochMilli()
                : null
        );
        eventDetailsMap.put(JOB_FINISHED_ISO_TIMESTAMP_KEY_NAME, job.getFinished().orElse(null));
        eventDetailsMap.put(JOB_GROUPING_KEY_NAME, job.getGrouping().orElse(null));
        eventDetailsMap.put(JOB_GROUPING_INSTANCE_KEY_NAME, job.getGroupingInstance().orElse(null));
        eventDetailsMap.put(JOB_STATUS_MESSAGE_KEY_NAME, job.getStatusMessage().orElse(null));
        eventDetailsMap.put(JOB_REQUESTED_MEMORY_KEY_NAME, job.getRequestedMemory().orElse(null));
        eventDetailsMap.put(JOB_API_CLIENT_HOSTNAME_KEY_NAME, job.getRequestApiClientHostname().orElse(null));
        eventDetailsMap.put(JOB_API_CLIENT_USER_AGENT_KEY_NAME, job.getRequestApiClientUserAgent().orElse(null));
        eventDetailsMap.put(JOB_AGENT_HOSTNAME_KEY_NAME, job.getRequestAgentClientHostname().orElse(null));
        eventDetailsMap.put(JOB_AGENT_VERSION_KEY_NAME, job.getRequestAgentClientVersion().orElse(null));
        eventDetailsMap.put(JOB_NUM_ATTACHMENTS_KEY_NAME, job.getNumAttachments().orElse(null));
        eventDetailsMap.put(JOB_EXIT_CODE_KEY_NAME, job.getExitCode().orElse(null));
        eventDetailsMap.put(JOB_ARCHIVE_LOCATION_KEY_NAME, job.getArchiveLocation().orElse(null));
        eventDetailsMap.put(JOB_USED_MEMORY_KEY_NAME, job.getMemoryUsed().orElse(null));
        eventDetailsMap.put(JOB_ARGUMENTS_KEY_NAME, job.getCommandArgs());

        if (job.getCommand().isPresent()) {
            final Command command = job.getCommand().get();
            eventDetailsMap.put(COMMAND_ID_KEY_NAME, command.getId());
            eventDetailsMap.put(COMMAND_NAME_KEY_NAME, command.getMetadata().getName());
            eventDetailsMap.put(COMMAND_VERSION_KEY_NAME, command.getMetadata().getVersion());
            eventDetailsMap.put(COMMAND_DESCRIPTION_KEY_NAME, command.getMetadata().getDescription().orElse(null));
            eventDetailsMap.put(COMMAND_CREATED_TIMESTAMP_KEY_NAME, command.getCreated().toEpochMilli());
            eventDetailsMap.put(COMMAND_CREATED_ISO_TIMESTAMP_KEY_NAME, command.getCreated());
            eventDetailsMap.put(COMMAND_UPDATED_TIMESTAMP_KEY_NAME, command.getUpdated().toEpochMilli());
            eventDetailsMap.put(COMMAND_UPDATED_ISO_TIMESTAMP_KEY_NAME, command.getUpdated());
            eventDetailsMap.put(COMMAND_EXECUTABLE_KEY_NAME, command.getExecutable());
        } else {
            eventDetailsMap.put(COMMAND_ID_KEY_NAME, null);
            eventDetailsMap.put(COMMAND_NAME_KEY_NAME, null);
            eventDetailsMap.put(COMMAND_VERSION_KEY_NAME, null);
            eventDetailsMap.put(COMMAND_DESCRIPTION_KEY_NAME, null);
            eventDetailsMap.put(COMMAND_CREATED_TIMESTAMP_KEY_NAME, null);
            eventDetailsMap.put(COMMAND_CREATED_ISO_TIMESTAMP_KEY_NAME, null);
            eventDetailsMap.put(COMMAND_UPDATED_TIMESTAMP_KEY_NAME, null);
            eventDetailsMap.put(COMMAND_UPDATED_ISO_TIMESTAMP_KEY_NAME, null);
            eventDetailsMap.put(COMMAND_EXECUTABLE_KEY_NAME, null);
        }

        if (job.getCluster().isPresent()) {
            final Cluster cluster = job.getCluster().get();
            eventDetailsMap.put(CLUSTER_ID_KEY_NAME, cluster.getId());
            eventDetailsMap.put(CLUSTER_NAME_KEY_NAME, cluster.getMetadata().getName());
            eventDetailsMap.put(CLUSTER_VERSION_KEY_NAME, cluster.getMetadata().getVersion());
            eventDetailsMap.put(CLUSTER_DESCRIPTION_KEY_NAME, cluster.getMetadata().getDescription().orElse(null));
            eventDetailsMap.put(CLUSTER_CREATED_TIMESTAMP_KEY_NAME, cluster.getCreated().toEpochMilli());
            eventDetailsMap.put(CLUSTER_CREATED_ISO_TIMESTAMP_KEY_NAME, cluster.getCreated());
            eventDetailsMap.put(CLUSTER_UPDATED_TIMESTAMP_KEY_NAME, cluster.getUpdated().toEpochMilli());
            eventDetailsMap.put(CLUSTER_UPDATED_ISO_TIMESTAMP_KEY_NAME, cluster.getUpdated());
        } else {
            eventDetailsMap.put(CLUSTER_ID_KEY_NAME, null);
            eventDetailsMap.put(CLUSTER_NAME_KEY_NAME, null);
            eventDetailsMap.put(CLUSTER_VERSION_KEY_NAME, null);
            eventDetailsMap.put(CLUSTER_DESCRIPTION_KEY_NAME, null);
            eventDetailsMap.put(CLUSTER_CREATED_TIMESTAMP_KEY_NAME, null);
            eventDetailsMap.put(CLUSTER_CREATED_ISO_TIMESTAMP_KEY_NAME, null);
            eventDetailsMap.put(CLUSTER_UPDATED_TIMESTAMP_KEY_NAME, null);
            eventDetailsMap.put(CLUSTER_UPDATED_ISO_TIMESTAMP_KEY_NAME, null);
        }

        eventDetailsMap.put(
            APPLICATIONS_KEY_NAME,
            job.getApplications().stream().map(
                application ->
                    application.getId() + COLON + application.getMetadata().getVersion()
            ).collect(Collectors.toList())
        );

        this.publishEvent(EventType.JOB_FINISHED, eventDetailsMap);
    }
}
