/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.tasks.leader;

import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Splitter;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.properties.ClusterCheckerProperties;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties;
import org.springframework.boot.actuate.health.Status;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A task which checks to see if this leader node can communicate with all other nodes in the cluster. If it can't
 * it will keep track of which nodes it can't communicate with and perform various actions based on the number of times
 * it can't communicate with that node. Currently (as of 3.0) this task will mark jobs as lost if they miss a certain
 * number of checks.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class ClusterCheckerTask extends LeadershipTask {
    private static final String PROPERTY_STATUS = "status";
    private static final String ERROR_COUNTS_GAUGE_METRIC_NAME = "genie.tasks.clusterChecker.errorCounts.gauge";
    private static final String LOST_JOBS_RATE_METRIC_NAME = "genie.tasks.clusterChecker.lostJobs.rate";
    private static final String FAILED_TO_UPDATE_RATE_METRIC_NAME = "genie.tasks.clusterChecker.unableToUpdateJob.rate";
    private static final String REMOTE_NODE_HEALTH_METRIC_NAME = "genie.tasks.clusterChecker.health.counter";

    private final String hostname;
    private final ClusterCheckerProperties properties;
    private final JobSearchService jobSearchService;
    private final JobPersistenceService jobPersistenceService;
    private final RestTemplate restTemplate;
    private final MeterRegistry registry;
    private final String scheme;
    private final String healthEndpoint;
    private final List<String> healthIndicatorsToIgnore;

    private final Map<String, Integer> errorCounts = new HashMap<>();

    private final Counter lostJobsCounter;
    private final Counter unableToUpdateJobCounter;

    /**
     * Constructor.
     *
     * @param genieHostInfo         Information about the host this Genie process is running on
     * @param properties            The properties to use to configure the task
     * @param jobSearchService      The job search service to use
     * @param jobPersistenceService The job persistence service to use
     * @param restTemplate          The rest template for http calls
     * @param webEndpointProperties The properties where Spring actuator is running
     * @param registry              The spectator registry for getting metrics
     */
    public ClusterCheckerTask(
        @NotNull final GenieHostInfo genieHostInfo,
        @NotNull final ClusterCheckerProperties properties,
        @NotNull final JobSearchService jobSearchService,
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final RestTemplate restTemplate,
        @NotNull final WebEndpointProperties webEndpointProperties,
        @NotNull final MeterRegistry registry
    ) {
        this.hostname = genieHostInfo.getHostname();
        this.properties = properties;
        this.jobSearchService = jobSearchService;
        this.jobPersistenceService = jobPersistenceService;
        this.restTemplate = restTemplate;
        this.registry = registry;
        this.scheme = this.properties.getScheme() + "://";
        this.healthEndpoint = ":" + this.properties.getPort() + webEndpointProperties.getBasePath() + "/health";
        this.healthIndicatorsToIgnore = Splitter.on(",").omitEmptyStrings()
            .trimResults().splitToList(properties.getHealthIndicatorsToIgnore());
        // Keep track of the number of nodes currently unreachable from the the master
        registry.gauge(ERROR_COUNTS_GAUGE_METRIC_NAME, this.errorCounts, Map::size);
        this.lostJobsCounter = registry.counter(LOST_JOBS_RATE_METRIC_NAME);
        this.unableToUpdateJobCounter = registry.counter(FAILED_TO_UPDATE_RATE_METRIC_NAME);
    }

    /**
     * Ping the health check endpoint of all other nodes which have running jobs. Track results.
     */
    @Override
    public void run() {
        log.info("Checking for cluster node health...");
        this.jobSearchService.getAllHostsWithActiveJobs()
            .stream()
            .filter(host -> !this.hostname.equals(host))
            .forEach(this::validateHostAndUpdateErrorCount);

        this.errorCounts.entrySet().removeIf(
            entry -> {
                final String host = entry.getKey();
                boolean result = true;
                if (entry.getValue() >= this.properties.getLostThreshold()) {
                    try {
                        this.updateJobsToFailedOnHost(host);
                    } catch (Exception e) {
                        log.error("Unable to update jobs on host {} due to exception", host, e);
                        this.unableToUpdateJobCounter.increment();
                        result = false;
                    }
                } else {
                    result = false;
                }
                return result;
            }
        );
        log.info("Finished checking for cluster node health.");
    }

    private void updateJobsToFailedOnHost(final String host) {
        final Set<Job> jobs = this.jobSearchService.getAllActiveJobsOnHost(host);
        jobs.forEach(
            job -> {
                try {
                    this.jobPersistenceService.setJobCompletionInformation(
                        job.getId().orElseThrow(IllegalArgumentException::new),
                        JobExecution.LOST_EXIT_CODE,
                        JobStatus.FAILED,
                        "Genie leader can't reach node running job. Assuming node and job are lost.",
                        null,
                        null
                    );
                    this.lostJobsCounter.increment();
                } catch (final GenieException ge) {
                    log.error("Unable to update job {} to failed due to exception", job.getId(), ge);
                    this.unableToUpdateJobCounter.increment();
                }
            }
        );
    }

    private void validateHostAndUpdateErrorCount(final String host) {
        //
        // If node is healthy, remove the entry from the errorCounts.
        // If node is not healthy, update the entry in errorCounts
        //
        if (this.isNodeHealthy(host)) {
            log.info("Host {} is no longer unhealthy", host);
            this.errorCounts.remove(host);
        } else {
            if (this.errorCounts.containsKey(host)) {
                final int currentCount = this.errorCounts.get(host) + 1;
                log.info("Host still unhealthy (check #{}): {}", host, currentCount);
                this.errorCounts.put(host, currentCount);
            } else {
                log.info("Marking host unhealthy: {}", host);
                this.errorCounts.put(host, 1);
            }
        }
    }

    private boolean isNodeHealthy(final String host) {
        //
        // A node is valid and healthy if all health indicators excluding the ones mentioned in healthIndicatorsToIgnore
        // are UP.
        //
        boolean result = true;
        try {
            this.restTemplate.getForObject(this.scheme + host + this.healthEndpoint, String.class);
        } catch (final HttpStatusCodeException e) {
            log.error("Failed validating host {}", host, e);
            try {
                final Map<String, Object> responseMap = GenieObjectMapper.getMapper()
                    .readValue(
                        e.getResponseBodyAsByteArray(),
                        TypeFactory.defaultInstance().constructMapType(Map.class, String.class, Object.class)
                    );
                for (Map.Entry<String, Object> responseEntry : responseMap.entrySet()) {
                    if (responseEntry.getValue() instanceof Map) {
                        final Map indicatorMap = (Map) responseEntry.getValue();

                        final String indicatorName = responseEntry.getKey();
                        final Object indicatorStatusOrNull = indicatorMap.get(PROPERTY_STATUS);

                        final Status indicatorStatus;

                        if (indicatorStatusOrNull instanceof Status) {
                            indicatorStatus = (Status) indicatorStatusOrNull;
                        } else if (indicatorStatusOrNull instanceof String) {
                            indicatorStatus = new Status((String) indicatorStatusOrNull);
                        } else {
                            indicatorStatus = Status.UNKNOWN;
                        }

                        //Increment counter tagged with target hostname and name of health indicator
                        final Set<Tag> tags = MetricsUtils.newSuccessTagsSet();
                        tags.add(Tag.of(MetricsConstants.TagKeys.HOST, host));
                        tags.add(Tag.of(MetricsConstants.TagKeys.HEALTH_INDICATOR, indicatorName));
                        tags.add(Tag.of(MetricsConstants.TagKeys.HEALTH_STATUS, indicatorStatus.getCode()));
                        this.registry.counter(REMOTE_NODE_HEALTH_METRIC_NAME, tags).increment();

                        if (this.healthIndicatorsToIgnore.contains(indicatorName)) {
                            log.debug("Ignoring indicator: {}", indicatorName);
                        } else if (Status.UP.equals(indicatorStatus)) {
                            log.debug("Indicator {} is UP", indicatorName);
                        } else {
                            log.warn("Indicator {} is {} for host {}", indicatorName, indicatorStatus, host);
                            // Mark host as failed but keep iterating to publish metrics.
                            result = false;
                        }
                    }
                }
            } catch (RuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
                log.error("Failed reading the error response when validating host {}", host, ex);
                result = false;
            }
        } catch (final Exception e) {
            log.error("Unable to reach {}", host, e);
            result = false;
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenieTaskScheduleType getScheduleType() {
        return GenieTaskScheduleType.FIXED_RATE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getFixedRate() {
        return this.properties.getRate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        this.errorCounts.clear();
    }

    /**
     * Get the current size of error counts. Mainly used for testing.
     *
     * @return Number of nodes currently in an error state
     */
    int getErrorCountsSize() {
        return this.errorCounts.size();
    }
}
