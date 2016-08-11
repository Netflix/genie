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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.web.properties.ClusterCheckerProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.ManagementServerProperties;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;
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
@Component
@Slf4j
public class ClusterCheckerTask extends LeadershipTask {
    private static final String PROPERTY_STATUS = "status";

    private final String hostName;
    private final ClusterCheckerProperties properties;
    private final JobSearchService jobSearchService;
    private final JobPersistenceService jobPersistenceService;
    private final RestTemplate restTemplate;
    private final String scheme;
    private final String healthEndpoint;
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<String> ignoreHealthEndPoints;

    private final Map<String, Integer> errorCounts = new HashMap<>();

    // TODO: Add metrics
    private final Counter lostJobsCounter;
    private final Counter unableToUpdateJobCounter;

    /**
     * Constructor.
     *
     * @param hostName                   The host name of this node
     * @param properties                 The properties to use to configure the task
     * @param jobSearchService           The job search service to use
     * @param jobPersistenceService      The job persistence service to use
     * @param restTemplate               The rest template for http calls
     * @param managementServerProperties The properties where Spring actuator is running
     * @param ignoreHealthEndPoints      Health end points to ignore when determining node's health
     * @param registry                   The spectator registry for getting metrics
     */
    @Autowired
    public ClusterCheckerTask(
        @NotNull final String hostName,
        @NotNull final ClusterCheckerProperties properties,
        @NotNull final JobSearchService jobSearchService,
        @NotNull final JobPersistenceService jobPersistenceService,
        @Qualifier("genieRestTemplate") @NotNull final RestTemplate restTemplate,
        @NotNull final ManagementServerProperties managementServerProperties,
        @Value("${genie.tasks.clusterChecker.ignoreHealthEndPoints:memory,genie}") final String ignoreHealthEndPoints,
        @NotNull final Registry registry
    ) {
        this.hostName = hostName;
        this.properties = properties;
        this.jobSearchService = jobSearchService;
        this.jobPersistenceService = jobPersistenceService;
        this.restTemplate = restTemplate;
        this.scheme = this.properties.getScheme() + "://";
        this.healthEndpoint = ":" + this.properties.getPort() + managementServerProperties.getContextPath() + "/health";
        this.ignoreHealthEndPoints = Splitter.on(",").omitEmptyStrings()
            .trimResults().splitToList(ignoreHealthEndPoints);
        // Keep track of the number of nodes currently unreachable from the the master
        registry.mapSize("genie.tasks.clusterChecker.errorCounts.gauge", this.errorCounts);
        this.lostJobsCounter = registry.counter("genie.tasks.clusterChecker.lostJobs.rate");
        this.unableToUpdateJobCounter = registry.counter("genie.tasks.clusterChecker.unableToUpdateJob.rate");
    }

    /**
     * Ping the health check endpoint of all other nodes which have running jobs. Track results.
     */
    @Override
    public void run() {
        log.info("Checking for cluster node health...");
        this.jobSearchService.getAllHostsWithActiveJobs()
            .stream()
            .filter(host -> !this.hostName.equals(host))
            .forEach(this::validateHostAndUpdateErrorCount);

        this.errorCounts.entrySet().removeIf(entry -> {
            final String host = entry.getKey();
            boolean result = true;
            if (entry.getValue() >= properties.getLostThreshold()) {
                try {
                    updateJobsToFailedOnHost(host);
                } catch (Exception e) {
                    log.error("Unable to update jobs on host {} due to exception", host, e);
                    unableToUpdateJobCounter.increment();
                    result = false;
                }
            } else {
                result = false;
            }
            return result;
        });
        log.info("Finished checking for cluster node health.");
    }

    private void updateJobsToFailedOnHost(final String host) {
        final Set<Job> jobs = jobSearchService.getAllActiveJobsOnHost(host);
        jobs.forEach(
            job -> {
                try {
                    jobPersistenceService.setJobCompletionInformation(
                        job.getId(),
                        JobExecution.LOST_EXIT_CODE,
                        JobStatus.FAILED,
                        "Genie leader can't reach node running job. Assuming node and job are lost."
                    );
                    lostJobsCounter.increment();
                } catch (final GenieException ge) {
                    log.error("Unable to update job {} to failed due to exception", job.getId(), ge);
                    Throwables.propagate(ge);
                }
            }
        );
    }

    private void validateHostAndUpdateErrorCount(final String host) {
        final boolean isNodeHealthy = isNodeHealthy(host);
        //
        // If node is healthy, remove the entry from the errorCounts.
        // If node is not healthy, update the entry in errorCounts
        //
        if (isNodeHealthy) {
            if (errorCounts.containsKey(host)) {
                errorCounts.remove(host);
            }
        } else {
            if (this.errorCounts.containsKey(host)) {
                this.errorCounts.put(host, this.errorCounts.get(host) + 1);
            } else {
                this.errorCounts.put(host, 1);
            }
        }
    }

    private boolean isNodeHealthy(final String host) {
        //
        // A node is valid and healthy if all health endpoints excluding the ones mentioned in ignoreHealthEndPoints
        // are UP.
        //
        boolean result = true;
        try {
            restTemplate.getForObject(this.scheme + host + this.healthEndpoint, String.class);
        } catch (final HttpStatusCodeException e) {
            log.error("Failed validating host {}", host, e);
            try {
                final Map<String, Object> responseMap = mapper.readValue(e.getResponseBodyAsByteArray(),
                    TypeFactory.defaultInstance().constructMapType(Map.class, String.class, Object.class));
                for (Map.Entry<String, Object> responseEntry : responseMap.entrySet()) {
                    if (responseEntry.getValue() instanceof Map
                        && !ignoreHealthEndPoints.contains(responseEntry.getKey())
                        && Status.OUT_OF_SERVICE.getCode()
                        .equals(((Map) responseEntry.getValue()).get(PROPERTY_STATUS))) {
                        result = false;
                    }
                }
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
     * Get the current size of error counts. Mainly used for testing.
     *
     * @return Number of nodes currently in an error state
     */
    protected int getErrorCountsSize() {
        return this.errorCounts.size();
    }
}
