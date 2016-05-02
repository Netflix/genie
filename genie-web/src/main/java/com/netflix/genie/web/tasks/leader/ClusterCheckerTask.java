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

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.web.properties.ClusterCheckerProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.ManagementServerProperties;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    private final String hostName;
    private final ClusterCheckerProperties properties;
    private final JobSearchService jobSearchService;
    private final JobPersistenceService jobPersistenceService;
    private final HttpClient httpClient;
    private final String scheme;
    private final String healthEndpoint;

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
     * @param httpClient                 The http client to use to send requests
     * @param managementServerProperties The properties where Spring actuator is running
     * @param registry                   The spectator registry for getting metrics
     */
    @Autowired
    public ClusterCheckerTask(
        @NotNull final String hostName,
        @NotNull final ClusterCheckerProperties properties,
        @NotNull final JobSearchService jobSearchService,
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final HttpClient httpClient,
        @NotNull final ManagementServerProperties managementServerProperties,
        @NotNull final Registry registry
    ) {
        this.hostName = hostName;
        this.properties = properties;
        this.jobSearchService = jobSearchService;
        this.jobPersistenceService = jobPersistenceService;
        this.httpClient = httpClient;
        this.scheme = this.properties.getScheme() + "://";
        this.healthEndpoint = ":" + this.properties.getPort() + managementServerProperties.getContextPath() + "/health";

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
        final Set<String> badNodes = new HashSet<>();
        this.jobSearchService.getAllHostsRunningJobs()
            .stream()
            .filter(host -> !this.hostName.equals(host))
            .forEach(
                host -> {
                    try {
                        final HttpGet get = new HttpGet(this.scheme + host + this.healthEndpoint);
                        final HttpResponse response = this.httpClient.execute(get);
                        if (response.getStatusLine().getStatusCode() != HttpStatus.OK.value()) {
                            badNodes.add(host);
                        }
                    } catch (final IOException ioe) {
                        log.error("Unable to reach {}", host, ioe);
                        badNodes.add(host);
                    }
                }
            );

        // Increment or add new bad nodes
        badNodes.stream().forEach(
            host -> {
                if (this.errorCounts.containsKey(host)) {
                    this.errorCounts.put(host, this.errorCounts.get(host) + 1);
                } else {
                    this.errorCounts.put(host, 1);
                }
            }
        );

        // Find any hosts that are now healthy since last iteration
        // Two loops to avoid concurrent modification exception
        final Set<String> toRemove = this.errorCounts.keySet()
            .stream()
            .filter(host -> !badNodes.contains(host))
            .collect(Collectors.toSet());
        toRemove.stream().forEach(this.errorCounts::remove);

        // Did we pass bad threshold on any hosts? Error jobs if so
        toRemove.clear();
        this.errorCounts.keySet()
            .stream()
            .filter(host -> this.errorCounts.get(host) == this.properties.getLostThreshold())
            .forEach(
                host -> {
                    toRemove.add(host);
                    final Set<JobExecution> jobs = this.jobSearchService.getAllRunningJobExecutionsOnHost(host);
                    jobs.stream().forEach(
                        job -> {
                            try {
                                this.jobPersistenceService.setExitCode(job.getId(), JobExecution.LOST_EXIT_CODE);
                                this.lostJobsCounter.increment();
                            } catch (final GenieException ge) {
                                log.error("Unable to update job {} to failed due to exception", job.getId(), ge);
                                this.unableToUpdateJobCounter.increment();
                            }
                        }
                    );
                }
            );
        // Remove the fields we just purged
        toRemove.stream().forEach(this.errorCounts::remove);
        log.info("Finished checking for cluster node health.");
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
