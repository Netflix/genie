/*
 *
 *  Copyright 2020 Netflix, Inc.
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.internal.util.ExponentialBackOffTrigger;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.springframework.scheduling.TaskScheduler;

import javax.validation.constraints.NotBlank;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link AgentRoutingService} that relies on Curator's Discovery extension.
 * Rather than the traditional use of this recipe (register a service for the node itself, this class registers one
 * service instance for each agent locally connected.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentRoutingServiceCuratorDiscoveryImpl implements AgentRoutingService {

    private static final String SERVICE_NAME = "agent_connections";
    private static final String METRICS_PREFIX = "genie.agents.connections.";
    private static final String CONNECTED_AGENTS_GAUGE_NAME = METRICS_PREFIX + "connected.gauge";
    private static final String REGISTERED_AGENTS_GAUGE_NAME = METRICS_PREFIX + "registered.gauge";
    private static final String ZOOKEEPER_SESSION_STATE_COUNTER_NAME = METRICS_PREFIX + "zookeeperSessionState.counter";
    private static final String AGENT_REGISTERED_TIMER_NAME = METRICS_PREFIX + "registered.timer";
    private static final String AGENT_UNREGISTERED_TIMER_NAME = METRICS_PREFIX + "unregistered.timer";
    private static final String AGENT_REFRESH_TIMER_NAME = METRICS_PREFIX + "refreshed.timer";
    private static final String AGENT_CONNECTED_COUNTER_NAME = METRICS_PREFIX + "connected.counter";
    private static final String AGENT_DISCONNECTED_COUNTER_NAME = METRICS_PREFIX + "disconnected.counter";
    private static final String ZK_CONNECTION_STATE_TAG_NAME = "connectionState";
    private static final Set<Tag> EMPTY_TAG_SET = ImmutableSet.of();
    private final String localHostname;
    private final ServiceDiscovery<Agent> serviceDiscovery;
    private final TaskScheduler taskScheduler;
    private final MeterRegistry registry;
    private final Set<String> connectedAgentsSet = Sets.newConcurrentHashSet();
    private final Map<String, ServiceInstance<Agent>> registeredAgentsMap = Maps.newConcurrentMap();
    private final ExponentialBackOffTrigger trigger = new ExponentialBackOffTrigger(
        ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_COMPLETION,
        100, // TODO make configurable
        5_000, // TODO make configurable
        1.2f // TODO make configurable
    );

    /**
     * Constructor.
     *
     * @param genieHostInfo                    The genie local host information
     * @param serviceDiscovery                 The service discovery client
     * @param taskScheduler                    The task scheduler
     * @param listenableCuratorConnectionState The listenable curator client connection status
     * @param registry                         The metrics registry
     */
    public AgentRoutingServiceCuratorDiscoveryImpl(
        final GenieHostInfo genieHostInfo,
        final ServiceDiscovery<Agent> serviceDiscovery,
        final TaskScheduler taskScheduler,
        final Listenable<ConnectionStateListener> listenableCuratorConnectionState,
        final MeterRegistry registry
    ) {
        this.localHostname = genieHostInfo.getHostname();
        this.serviceDiscovery = serviceDiscovery;
        this.taskScheduler = taskScheduler;
        this.registry = registry;

        // Schedule periodic reconciliation between in-memory connected set and Service Discovery state
        this.taskScheduler.schedule(this::reconcileRegistrationsTask, trigger);

        // Listen for Curator session state changes
        listenableCuratorConnectionState.addListener(this::handleConnectionStateChange);

        // Create gauge metric for agents connected and registered
        registry.gauge(CONNECTED_AGENTS_GAUGE_NAME, EMPTY_TAG_SET, this.connectedAgentsSet, Set::size);
        registry.gaugeMapSize(REGISTERED_AGENTS_GAUGE_NAME, EMPTY_TAG_SET, this.registeredAgentsMap);
    }

    private void handleConnectionStateChange(final CuratorFramework client, final ConnectionState newState) {

        this.registry.counter(
            ZOOKEEPER_SESSION_STATE_COUNTER_NAME,
            Sets.newHashSet(Tag.of(ZK_CONNECTION_STATE_TAG_NAME, newState.name()))
        ).increment();

        switch (newState) {
            case CONNECTED:
                // Immediately schedule a reconciliation
                log.info("Zookeeper/Curator connected (or re-connected)");
                this.taskScheduler.schedule(this::reconcileRegistrationsTask, Instant.now());
                break;

            case LOST:
                // When session expires, all ephemeral nodes disappear
                log.info("Zookeeper/Curator session expired, all instances will need to re-register");
                this.registeredAgentsMap.clear();
                break;

            default:
                log.debug("Zookeeper/Curator connection state changed to: {}", newState);
        }
    }

    private synchronized void reconcileRegistrationsTask() {
        try {
            this.reconcileRegistrations();
        } catch (Exception e) {
            log.error("Unexpected exception in reconciliation task", e);
        }
    }

    private void reconcileRegistrations() {
        boolean anyChange = false;

        // Register all agent connections that are not already registered
        for (final String jobId : this.connectedAgentsSet) {
            if (!this.registeredAgentsMap.containsKey(jobId)) {
                final ServiceInstance<Agent> serviceInstance = new ServiceInstance<>(
                    SERVICE_NAME,
                    jobId,
                    localHostname,
                    null,
                    null,
                    new Agent(jobId),
                    Instant.now().getEpochSecond(),
                    ServiceType.DYNAMIC,
                    null
                );

                Set<Tag> tags = MetricsUtils.newSuccessTagsSet();
                final long start = System.nanoTime();
                try {
                    this.serviceDiscovery.registerService(serviceInstance);
                    this.registeredAgentsMap.put(jobId, serviceInstance);
                    anyChange = true;
                } catch (Exception e) {
                    log.error("Failed to register agent executing job: {}", jobId, e);
                    tags = MetricsUtils.newFailureTagsSetForException(e);
                } finally {
                    this.registry.timer(
                        AGENT_REGISTERED_TIMER_NAME,
                        tags
                    ).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                }
            }
        }

        // Unregister all agent connections that are no longer active
        for (final Map.Entry<String, ServiceInstance<Agent>> entry : this.registeredAgentsMap.entrySet()) {
            if (!this.connectedAgentsSet.contains(entry.getKey())) {

                final String jobId = entry.getKey();
                final ServiceInstance<Agent> serviceInstance = entry.getValue();

                Set<Tag> tags = MetricsUtils.newSuccessTagsSet();
                final long start = System.nanoTime();
                try {
                    this.serviceDiscovery.unregisterService(serviceInstance);
                    this.registeredAgentsMap.remove(jobId);
                    anyChange = true;
                } catch (Exception e) {
                    log.error("Failed to unregister agent executing job id: {}", jobId);
                    tags = MetricsUtils.newFailureTagsSetForException(e);
                } finally {
                    this.registry.timer(
                        AGENT_UNREGISTERED_TIMER_NAME,
                        tags
                    ).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                }
            }
        }

        // Update all agent connections that are expected to exist.
        // This in necessary in case an entry was removed by a different node where the agent was previously connected.
        for (final Map.Entry<String, ServiceInstance<Agent>> entry : this.registeredAgentsMap.entrySet()) {
            if (this.connectedAgentsSet.contains(entry.getKey())) {

                final String jobId = entry.getKey();
                final ServiceInstance<Agent> serviceInstance = entry.getValue();

                Set<Tag> tags = MetricsUtils.newSuccessTagsSet();
                final long start = System.nanoTime();
                try {
                    this.serviceDiscovery.updateService(serviceInstance);
                } catch (Exception e) {
                    log.error("Failed to refresh agent executing job id: {}", jobId);
                    tags = MetricsUtils.newFailureTagsSetForException(e);
                } finally {
                    this.registry.timer(
                        AGENT_REFRESH_TIMER_NAME,
                        tags
                    ).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                }
            }
        }

        // If anything changed, reset the backoff so this task runs again soon.
        if (anyChange) {
            this.trigger.reset();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleClientConnected(@NotBlank final String jobId) {
        this.registry.counter(
            AGENT_CONNECTED_COUNTER_NAME,
            EMPTY_TAG_SET
        ).increment();

        // Add to connected set
        this.connectedAgentsSet.add(jobId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleClientDisconnected(@NotBlank final String jobId) {
        this.registry.counter(
            AGENT_DISCONNECTED_COUNTER_NAME,
            EMPTY_TAG_SET
        ).increment();

        // Remove from connected set
        this.connectedAgentsSet.remove(jobId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getHostnameForAgentConnection(@NotBlank final String jobId) {
        log.debug("Looking up agent executing job: {}", jobId);
        if (isAgentConnectionLocal(jobId)) {
            return Optional.of(localHostname);
        }

        final ServiceInstance<Agent> instance;
        try {
            instance = serviceDiscovery.queryForInstance(SERVICE_NAME, jobId);
        } catch (Exception e) {
            log.error("Error looking up agent connection for job {}", jobId, e);
            return Optional.empty();
        }

        if (instance == null) {
            log.warn("Could not find agent connection for job {}", jobId);
            return Optional.empty();
        }

        return Optional.ofNullable(instance.getAddress());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAgentConnectionLocal(@NotBlank final String jobId) {
        return this.connectedAgentsSet.contains(jobId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAgentConnected(final String jobId) {
        return this.getHostnameForAgentConnection(jobId).isPresent();
    }

    /**
     * Payload for typed {@link ServiceDiscovery}.
     */
    @Getter
    @EqualsAndHashCode
    public static final class Agent {
        // This field is superfluous, but added because serialization fails on empty beans
        private final String jobId;

        // Jackson deserialization requires this dummy constructor
        private Agent() {
            this.jobId = null;
        }

        /**
         * Constructor.
         *
         * @param jobId The job id
         */
        public Agent(@JsonProperty(value = "jobId", required = true) final String jobId) {
            this.jobId = jobId;
        }
    }
}
