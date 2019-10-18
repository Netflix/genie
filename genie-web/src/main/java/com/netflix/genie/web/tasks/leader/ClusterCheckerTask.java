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

import com.google.common.base.Splitter;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.data.services.AgentConnectionPersistenceService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.properties.ClusterCheckerProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties;
import org.springframework.boot.actuate.health.Status;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.validation.constraints.NotNull;
import java.io.IOException;
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
    private static final String UNHEALTHY_HOSTS_GAUGE_METRIC_NAME = "genie.tasks.clusterChecker.unhealthyHosts.gauge";
    private static final String BAD_HEALTH_COUNT_METRIC_NAME = "genie.tasks.clusterChecker.failedHealthcheck.counter";
    private static final String BAD_RESPONSE_COUNT_METRIC_NAME = "genie.tasks.clusterChecker.invalidResponse.counter";
    private static final String BAD_HOST_COUNT_METRIC_NAME = "genie.tasks.clusterChecker.unreachableHost.counter";
    private static final String FAILED_JOBS_COUNT_METRIC_NAME = "genie.tasks.clusterChecker.jobsMarkedFailed.counter";
    private static final String REAPED_CONNECTIONS_METRIC_NAME = "genie.tasks.clusterChecker.connectionsReaped.counter";

    private final String hostname;
    private final ClusterCheckerProperties properties;
    private final JobSearchService jobSearchService;
    private final JobPersistenceService jobPersistenceService;
    private final AgentConnectionPersistenceService agentConnectionPersistenceService;
    private final RestTemplate restTemplate;
    private final MeterRegistry registry;
    private final String scheme;
    private final String healthEndpoint;
    private final List<String> healthIndicatorsToIgnore;

    private final Map<String, Integer> errorCounts = new HashMap<>();

    /**
     * Constructor.
     *
     * @param genieHostInfo                     Information about the host this Genie process is running on
     * @param properties                        The properties to use to configure the task
     * @param jobSearchService                  The job search service to use
     * @param jobPersistenceService             The job persistence service to use
     * @param agentConnectionPersistenceService The agent connections persistence service
     * @param restTemplate                      The rest template for http calls
     * @param webEndpointProperties             The properties where Spring actuator is running
     * @param registry                          The spectator registry for getting metrics
     */
    public ClusterCheckerTask(
        @NotNull final GenieHostInfo genieHostInfo,
        @NotNull final ClusterCheckerProperties properties,
        @NotNull final JobSearchService jobSearchService,
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final AgentConnectionPersistenceService agentConnectionPersistenceService,
        @NotNull final RestTemplate restTemplate,
        @NotNull final WebEndpointProperties webEndpointProperties,
        @NotNull final MeterRegistry registry
    ) {
        this.hostname = genieHostInfo.getHostname();
        this.properties = properties;
        this.jobSearchService = jobSearchService;
        this.jobPersistenceService = jobPersistenceService;
        this.agentConnectionPersistenceService = agentConnectionPersistenceService;
        this.restTemplate = restTemplate;
        this.registry = registry;
        this.scheme = this.properties.getScheme() + "://";
        this.healthEndpoint = ":" + this.properties.getPort() + webEndpointProperties.getBasePath() + "/health";
        this.healthIndicatorsToIgnore = Splitter.on(",").omitEmptyStrings()
            .trimResults().splitToList(properties.getHealthIndicatorsToIgnore());
        // Keep track of the number of nodes currently unreachable from the the master
        Gauge.builder(UNHEALTHY_HOSTS_GAUGE_METRIC_NAME, this.errorCounts, Map::size)
            .register(registry);
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
                        result = false;
                    }
                    try {
                        this.cleanupAgentConnectionsToHost(host);
                    } catch (RuntimeException e) {
                        log.error("Unable to drop agent connections to host {} due to exception", host, e);
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
                final Set<Tag> tags = MetricsUtils.newSuccessTagsSet();
                tags.add(Tag.of(MetricsConstants.TagKeys.HOST, host));
                try {
                    this.jobPersistenceService.setJobCompletionInformation(
                        job.getId().orElseThrow(IllegalArgumentException::new),
                        JobExecution.LOST_EXIT_CODE,
                        JobStatus.FAILED,
                        "Genie leader can't reach node running job. Assuming node and job are lost.",
                        null,
                        null
                    );
                } catch (final GenieException ge) {
                    MetricsUtils.addFailureTagsWithException(tags, ge);
                    log.error("Unable to update job {} to failed due to exception", job.getId(), ge);
                    throw new RuntimeException("Failed to update job", ge);
                } finally {
                    // Increment whenever there is an attempt to mark a job failed
                    // (and tag whether it was successful or not).
                    registry.counter(FAILED_JOBS_COUNT_METRIC_NAME, tags).increment();
                }
            }
        );
    }

    private void cleanupAgentConnectionsToHost(final String host) {
        final Set<Tag> tags = MetricsUtils.newSuccessTagsSet();
        tags.add(Tag.of(MetricsConstants.TagKeys.HOST, host));
        int reapedConnectionsCount = 1;
        try {
            reapedConnectionsCount = this.agentConnectionPersistenceService.removeAllAgentConnectionToServer(host);
            log.info("Dropped {} agent connections to host {}", reapedConnectionsCount, host);
        } catch (RuntimeException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            log.error("Unable to drop agent connections to host {}", host, e);
            throw e;
        } finally {
            // Increment whenever there is an attept to reap connection
            // (and tag wether it was successful or not).
            this.registry.counter(REAPED_CONNECTIONS_METRIC_NAME, tags).increment(reapedConnectionsCount);
        }
    }

    private void validateHostAndUpdateErrorCount(final String host) {
        //
        // If node is healthy, remove the entry from the errorCounts.
        // If node is not healthy, update the entry in errorCounts
        //
        if (this.isNodeHealthy(host)) {
            if (this.errorCounts.remove(host) != null) {
                log.info("Host {} is no longer unhealthy", host);
            }
        } else {
            if (this.errorCounts.containsKey(host)) {
                final int currentCount = this.errorCounts.get(host) + 1;
                log.info("Host still unhealthy (check #{}): {}", currentCount, host);
                this.errorCounts.put(host, currentCount);
            } else {
                log.info("Marking host unhealthy: {}", host);
                this.errorCounts.put(host, 1);
            }
        }
    }

    private boolean isNodeHealthy(final String host) {
        // A node is valid and healthy if all health indicators excluding the ones mentioned in healthIndicatorsToIgnore
        // are UP.
        String responseContent;
        try {
            responseContent = this.restTemplate.getForObject(
                this.scheme + host + this.healthEndpoint,
                String.class
            );
            log.debug("Healtcheck retrieved successfully from: {}", host);
        } catch (final HttpStatusCodeException e) {
            // Healthcheck returning status != 2xx should still contain a payload with health details.
            log.warn("Host {} healthcheck returned code: {}", host, e.getStatusCode(), e);
            responseContent = e.getResponseBodyAsString();
        } catch (final RestClientException e) {
            // Other failure to execute the request
            log.warn("Unable to request healtcheck response from host: {}", host, e);
            this.registry.counter(BAD_HOST_COUNT_METRIC_NAME, MetricsConstants.TagKeys.HOST, host).increment();
            return false;
        }

        // Parse response body
        final HealthEndpointResponse healthEndpointResponse;
        try {
            healthEndpointResponse = GenieObjectMapper.getMapper()
                .readValue(
                    responseContent,
                    HealthEndpointResponse.class
                );
        } catch (IOException ex) {
            log.warn("Failed to parse healthcheck response from host: {}: {}", host, ex.getMessage());
            this.registry.counter(BAD_RESPONSE_COUNT_METRIC_NAME, MetricsConstants.TagKeys.HOST, host).increment();
            return false;
        }

        // Ignore the top-level health (it's not UP if this code is executing) and instead look at individual
        // indicators, as some of them may be ignored.
        boolean hostHealthy = true;
        for (
            final Map.Entry<String, HealthIndicatorDetails> entry : healthEndpointResponse.getComponents().entrySet()
        ) {
            final String healthIndicatorName = entry.getKey();
            final HealthIndicatorDetails healthIndicator = entry.getValue();

            if (this.healthIndicatorsToIgnore.contains(healthIndicatorName)) {
                log.debug("Ignoring indicator: {}", healthIndicatorName);
            } else if (Status.UP.getCode().equals(healthIndicator.getStatus().getCode())) {
                log.debug("Indicator {} is UP", healthIndicatorName);
            } else {
                // Consider this host unhealthy, but keep going to publish metrics for all indicators
                hostHealthy = false;
                //Increment counter tagged with target hostname and name of health indicator
                this.registry.counter(
                    BAD_HEALTH_COUNT_METRIC_NAME,
                    MetricsConstants.TagKeys.HOST, host,
                    MetricsConstants.TagKeys.HEALTH_INDICATOR, healthIndicatorName,
                    MetricsConstants.TagKeys.HEALTH_STATUS, healthIndicator.getStatus().getCode()
                ).increment();
            }
        }
        return hostHealthy;
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

    @Getter
    @Setter
    @NoArgsConstructor
    private static class HealthIndicatorDetails {
        private Status status;
        private Map<String, Object> details;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    private static class HealthEndpointResponse {
        private Status status;
        private Map<String, HealthIndicatorDetails> components;
    }
}
