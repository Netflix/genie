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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.properties.AgentRoutingServiceProperties;
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
import org.apache.zookeeper.KeeperException;
import org.springframework.scheduling.TaskScheduler;

import javax.validation.constraints.NotBlank;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    private static final String AGENT_LOOKUP_TIMER_NAME = METRICS_PREFIX + "lookup.timer";
    private static final String ZK_CONNECTION_STATE_TAG_NAME = "connectionState";
    private static final String ROUTE_FOUND_TAG_NAME = "found";
    private static final Set<Tag> EMPTY_TAG_SET = ImmutableSet.of();
    private final String localHostname;
    private final ServiceDiscovery<Agent> serviceDiscovery;
    private final TaskScheduler taskScheduler;
    private final MeterRegistry registry;
    private final AgentRoutingServiceProperties properties;
    private final Set<String> connectedAgentsSet;
    private final Map<String, ServiceInstance<Agent>> registeredAgentsMap;
    private final PriorityBlockingQueue<RegisterMutation> registrationQueue;
    private final AtomicReference<Thread> registrationTaskThread;
    private final ThreadFactory threadFactory;

    /**
     * Constructor.
     *
     * @param genieHostInfo                    The genie local host information
     * @param serviceDiscovery                 The service discovery client
     * @param taskScheduler                    The task scheduler
     * @param listenableCuratorConnectionState The listenable curator client connection status
     * @param registry                         The metrics registry
     * @param properties                       The service properties
     */
    public AgentRoutingServiceCuratorDiscoveryImpl(
        final GenieHostInfo genieHostInfo,
        final ServiceDiscovery<Agent> serviceDiscovery,
        final TaskScheduler taskScheduler,
        final Listenable<ConnectionStateListener> listenableCuratorConnectionState,
        final MeterRegistry registry,
        final AgentRoutingServiceProperties properties
    ) {
        this(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableCuratorConnectionState,
            registry,
            properties,
            new ThreadFactory() {
                private final AtomicLong threadCounter = new AtomicLong();

                @Override
                public Thread newThread(final Runnable r) {
                    return new Thread(
                        r,
                        this.getClass().getSimpleName() + "-registration-" + threadCounter.incrementAndGet()
                    );
                }
            }
        );
    }

    @VisibleForTesting
    AgentRoutingServiceCuratorDiscoveryImpl(
        final GenieHostInfo genieHostInfo,
        final ServiceDiscovery<Agent> serviceDiscovery,
        final TaskScheduler taskScheduler,
        final Listenable<ConnectionStateListener> listenableCuratorConnectionState,
        final MeterRegistry registry,
        final AgentRoutingServiceProperties properties,
        final ThreadFactory threadFactory
    ) {
        this.localHostname = genieHostInfo.getHostname();
        this.serviceDiscovery = serviceDiscovery;
        this.taskScheduler = taskScheduler;
        this.registry = registry;
        this.threadFactory = threadFactory;
        this.properties = properties;
        this.registeredAgentsMap = Maps.newConcurrentMap();
        this.connectedAgentsSet = Sets.newConcurrentHashSet();
        this.registrationQueue = new PriorityBlockingQueue<>();
        this.registrationTaskThread = new AtomicReference<>();

        // Create gauge metric for agents connected and registered
        registry.gauge(CONNECTED_AGENTS_GAUGE_NAME, EMPTY_TAG_SET, this.connectedAgentsSet, Set::size);
        registry.gaugeMapSize(REGISTERED_AGENTS_GAUGE_NAME, EMPTY_TAG_SET, this.registeredAgentsMap);

        // Listen for Curator session state changes
        listenableCuratorConnectionState.addListener(this::handleConnectionStateChange);

        // The curator client is passed already connected.
        // See: org.springframework.cloud.zookeeper.ZookeeperAutoConfiguration
        this.startRegistrationThread();
    }

    private void startRegistrationThread() {
        final Thread newThread = this.threadFactory.newThread(this::registrationTask);
        final Thread oldThread = this.registrationTaskThread.getAndSet(newThread);
        if (oldThread != null) {
            oldThread.interrupt();
        }
        newThread.start();
    }

    private void stopRegistrationThread() {
        final Thread thread = this.registrationTaskThread.getAndSet(null);
        if (thread != null) {
            thread.interrupt();
        }
    }

    // Thread task that consumes registration queue items and applies the corresponding mutation with Curator client.
    // This thread is stopped with an interrupt if the client is disconnected.
    private void registrationTask() {
        while (true) {
            try {
                processNextRegistrationMutation();
            } catch (InterruptedException e) {
                break;
            }
        }

        log.debug("Registration thread terminating");
    }

    private void processNextRegistrationMutation() throws InterruptedException {
        RegisterMutation mutation = null;
        try {
            // Blocking
            mutation = this.registrationQueue.take();

            final String jobId = mutation.getJobId();
            // Check if agent is still connected by the time this mutation is taken from the queue to
            // be processed.
            final boolean agentIsConnected = this.connectedAgentsSet.contains(mutation.getJobId());

            if (agentIsConnected) {
                // Register or re-register agent connection
                if (mutation.isRefresh()) {
                    refreshAgentConnection(jobId);
                } else {
                    registerAgentConnection(jobId);
                }

                // Schedule a future refresh for this agent connection
                this.taskScheduler.schedule(
                    () -> this.registrationQueue.add(RegisterMutation.refresh(jobId)),
                    Instant.now().plus(this.properties.getRefreshInterval())
                );

            } else {
                // Unregister agent connection
                unregisterAgentConnection(jobId);
            }

        } catch (InterruptedException e) {
            log.warn("Registration task interrupted", e);
            if (mutation != null) {
                // Re-enqueue mutation that was in-progress when interrupted
                this.registrationQueue.add(mutation);
            }
            throw e;
        }
    }

    private void registerAgentConnection(final String jobId) throws InterruptedException {
        log.debug("Registering route for job: {}", jobId);

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
        } catch (InterruptedException e) {
            // Ensure interrupt is not swallowed by the generic catch
            log.debug("Interrupted while registering {}", jobId);
            tags = MetricsUtils.newFailureTagsSetForException(e);
            throw e;
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

    private void refreshAgentConnection(final String jobId) throws InterruptedException {
        log.debug("Refreshing route for job: {}", jobId);

        final ServiceInstance<Agent> serviceInstance = this.registeredAgentsMap.get(jobId);

        if (serviceInstance == null) {
            log.warn("Instance record not found for job {}", jobId);
            this.registerAgentConnection(jobId);
            return;
        }

        Set<Tag> tags = MetricsUtils.newSuccessTagsSet();
        final long start = System.nanoTime();
        try {
            this.serviceDiscovery.updateService(serviceInstance);
        } catch (KeeperException.NoNodeException e) {
            log.warn("Failed to update registration of agent executing job id: {}", jobId);
            // Failed because expected existing node is not present. Create it.
            this.registerAgentConnection(jobId);
        } catch (InterruptedException e) {
            // Ensure interrupt is not swallowed by the generic catch
            log.debug("Interrupted while refreshing {}", jobId);
            tags = MetricsUtils.newFailureTagsSetForException(e);
            throw e;
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

    private void unregisterAgentConnection(final String jobId) throws InterruptedException {
        final ServiceInstance<Agent> serviceInstance = this.registeredAgentsMap.get(jobId);

        if (serviceInstance == null) {
            log.debug("Skipping unregistration, already removed");
            return;
        }

        log.debug("Unregistering route for job: {}", jobId);

        Set<Tag> tags = MetricsUtils.newSuccessTagsSet();
        final long start = System.nanoTime();
        try {
            this.serviceDiscovery.unregisterService(serviceInstance);
            this.registeredAgentsMap.remove(jobId);
        } catch (InterruptedException e) {
            // Ensure interrupt is not swallowed by the generic catch
            log.debug("Interrupted while unregistering {}", jobId);
            tags = MetricsUtils.newFailureTagsSetForException(e);
            throw e;
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

    private void handleConnectionStateChange(final CuratorFramework client, final ConnectionState newState) {

        this.registry.counter(
            ZOOKEEPER_SESSION_STATE_COUNTER_NAME,
            Sets.newHashSet(Tag.of(ZK_CONNECTION_STATE_TAG_NAME, newState.name()))
        ).increment();

        log.info("Zookeeper/Curator client: {}", newState);

        switch (newState) {
            case CONNECTED:
            case RECONNECTED:
                startRegistrationThread();
                break;

            case LOST:
            case SUSPENDED:
                stopRegistrationThread();
                break;

            default:
                log.warn("Zookeeper/Curator unhandled connection state: {}", newState);
        }
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

        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        String address = null;

        try {
            final ServiceInstance<Agent> instance = serviceDiscovery.queryForInstance(SERVICE_NAME, jobId);
            if (instance == null) {
                log.debug("Could not find agent connection for job {}", jobId);
            } else {
                address = instance.getAddress();
            }
            MetricsUtils.addSuccessTags(tags);
        } catch (Exception e) {
            log.error("Error looking up agent connection for job {}", jobId, e);
            address = null;
            MetricsUtils.addFailureTagsWithException(tags, e);
        } finally {
            tags.add(Tag.of(ROUTE_FOUND_TAG_NAME, String.valueOf(address != null)));
            this.registry.timer(
                AGENT_LOOKUP_TIMER_NAME,
                tags
            ).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }

        return Optional.ofNullable(address);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAgentConnectionLocal(@NotBlank final String jobId) {
        return this.connectedAgentsSet.contains(jobId);
    }

    @Override
    public void handleClientConnected(@NotBlank final String jobId) {
        log.debug("Adding to routing table (pending registration): {}", jobId);

        final boolean isNew = this.connectedAgentsSet.add(jobId);
        this.registrationQueue.add(RegisterMutation.update(jobId));

        if (isNew) {
            this.registry.counter(AGENT_CONNECTED_COUNTER_NAME).increment();
        }
    }

    @Override
    public void handleClientDisconnected(@NotBlank final String jobId) {
        log.debug("Removing from routing table (pending un-registration): {}", jobId);

        final boolean removed = this.connectedAgentsSet.remove(jobId);
        this.registrationQueue.add(RegisterMutation.update(jobId));

        if (removed) {
            this.registry.counter(AGENT_DISCONNECTED_COUNTER_NAME).increment();
        }
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

    @Getter
    @EqualsAndHashCode
    private static final class RegisterMutation implements Comparable<RegisterMutation> {
        private final String jobId;
        private final boolean refresh;
        private final long timestamp;

        private RegisterMutation(final String jobId, final boolean refresh) {
            this.jobId = jobId;
            this.refresh = refresh;
            this.timestamp = System.nanoTime();
        }

        static RegisterMutation refresh(final String jobId) {
            return new RegisterMutation(jobId, true);
        }

        static RegisterMutation update(final String jobId) {
            return new RegisterMutation(jobId, false);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int compareTo(final RegisterMutation other) {
            if (this.isRefresh() == other.isRefresh()) {
                final long timestampDifference = this.getTimestamp() - other.getTimestamp();
                if (timestampDifference == 0) {
                    return this.getJobId().compareTo(other.getJobId());
                } else {
                    return timestamp > 0 ? 1 : -1;
                }
            } else {
                return this.isRefresh() ? 1 : -1;
            }
        }
    }
}
