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
package com.netflix.genie.web.agent.services.impl

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.genie.common.external.util.GenieObjectMapper
import com.netflix.genie.common.internal.util.GenieHostInfo
import com.netflix.genie.web.agent.services.AgentRoutingService
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.listen.Listenable
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.curator.x.discovery.ServiceDiscovery
import org.apache.curator.x.discovery.ServiceInstance
import org.apache.curator.x.discovery.ServiceType
import org.apache.zookeeper.KeeperException
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.Trigger
import spock.lang.Specification

import java.time.Instant
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class AgentRoutingServiceCuratorDiscoveryImplSpec extends Specification {
    GenieHostInfo genieHostInfo
    ServiceDiscovery<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceDiscovery
    TaskScheduler taskScheduler
    Listenable<ConnectionStateListener> listenableConnectionState
    MeterRegistry meterRegistry
    ScheduledFuture<?> scheduledFuture
    String localHostname
    Counter counter
    Timer timer

    void setup() {
        this.genieHostInfo = Mock(GenieHostInfo)
        this.serviceDiscovery = Mock(ServiceDiscovery)
        this.taskScheduler = Mock(TaskScheduler)
        this.listenableConnectionState = Mock(Listenable)
        this.meterRegistry = Mock(MeterRegistry)
        this.scheduledFuture = Mock(ScheduledFuture)
        this.localHostname = UUID.randomUUID().toString()
        this.counter = Mock(Counter)
        this.timer = Mock(Timer)
    }

    def "Agent service instance POJO can serialize and deserialize"() {
        setup:
        AgentRoutingServiceCuratorDiscoveryImpl.Agent agent = new AgentRoutingServiceCuratorDiscoveryImpl.Agent(UUID.randomUUID().toString())
        ObjectMapper mapper = GenieObjectMapper.getMapper()

        when:
        byte[] bytes = mapper.writeValueAsBytes(agent)

        then:
        bytes != null

        when:
        AgentRoutingServiceCuratorDiscoveryImpl.Agent agent2 = mapper.readValue(bytes, AgentRoutingServiceCuratorDiscoveryImpl.Agent)

        then:
        agent == agent2
    }

    def "Connect and disconnect without errors"() {
        setup:
        String jobId1 = UUID.randomUUID().toString()
        String jobId2 = UUID.randomUUID().toString()

        ConnectionStateListener listener
        Runnable reconciliationTask
        ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceInstanceJob1
        ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceInstanceJob2

        when:
        AgentRoutingService agentRoutingService = new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableConnectionState,
            meterRegistry
        )

        then:
        1 * genieHostInfo.getHostname() >> localHostname
        1 * listenableConnectionState.addListener(_ as ConnectionStateListener) >> {
            args ->
                listener = args[0] as ConnectionStateListener
        }
        1 * taskScheduler.schedule(_ as Runnable, _ as Trigger) >> {
            args ->
                reconciliationTask = args[0] as Runnable
                return scheduledFuture
        }
        1 * meterRegistry.gauge(AgentRoutingServiceCuratorDiscoveryImpl.CONNECTED_AGENTS_GAUGE_NAME, _, _, _)
        1 * meterRegistry.gaugeMapSize(AgentRoutingServiceCuratorDiscoveryImpl.REGISTERED_AGENTS_GAUGE_NAME, _, _)
        listener != null
        reconciliationTask != null

        when:
        agentRoutingService.handleClientConnected(jobId1)
        agentRoutingService.handleClientConnected(jobId2)
        agentRoutingService.handleClientDisconnected(jobId2)

        then:
        2 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_CONNECTED_COUNTER_NAME, _) >> counter
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_DISCONNECTED_COUNTER_NAME, _) >> counter
        3 * counter.increment()

        when:
        reconciliationTask.run()

        then:
        1 * serviceDiscovery.registerService(_ as ServiceInstance) >> {
            args ->
                serviceInstanceJob1 = args[0] as ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent>
                return
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        serviceInstanceJob1 != null
        serviceInstanceJob1.getServiceType() == ServiceType.DYNAMIC
        serviceInstanceJob1.getId() == jobId1
        serviceInstanceJob1.getAddress() == localHostname
        serviceInstanceJob1.getPayload().getJobId() == jobId1
        serviceInstanceJob1.getName() == AgentRoutingServiceCuratorDiscoveryImpl.SERVICE_NAME
        1 * serviceDiscovery.updateService(_ as ServiceInstance) >> {
            args ->
                assert serviceInstanceJob1 == args[0]
                return
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REFRESH_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)

        when:
        agentRoutingService.handleClientDisconnected(jobId1)
        agentRoutingService.handleClientConnected(jobId2)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_CONNECTED_COUNTER_NAME, _) >> counter
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_DISCONNECTED_COUNTER_NAME, _) >> counter
        2 * counter.increment()

        when:
        reconciliationTask.run()

        then:
        1 * serviceDiscovery.registerService(_ as ServiceInstance) >> {
            args ->
                serviceInstanceJob2 = args[0] as ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent>
                return
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * serviceDiscovery.unregisterService(serviceInstanceJob1)
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_UNREGISTERED_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        serviceInstanceJob2 != null
        serviceInstanceJob2.getServiceType() == ServiceType.DYNAMIC
        serviceInstanceJob2.getId() == jobId2
        serviceInstanceJob2.getAddress() == localHostname
        serviceInstanceJob2.getPayload().getJobId() == jobId2
        serviceInstanceJob2.getName() == AgentRoutingServiceCuratorDiscoveryImpl.SERVICE_NAME
        1 * serviceDiscovery.updateService(_ as ServiceInstance) >> {
            args ->
                assert serviceInstanceJob2 == args[0]
                return
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REFRESH_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
    }


    def "Registration and un-registration errors"() {
        setup:
        String jobId = UUID.randomUUID().toString()

        ConnectionStateListener listener
        Runnable reconciliationTask
        ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceInstanceJob

        when:
        AgentRoutingService agentRoutingService = new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableConnectionState,
            meterRegistry
        )

        then:
        1 * genieHostInfo.getHostname() >> localHostname
        1 * listenableConnectionState.addListener(_ as ConnectionStateListener) >> {
            args ->
                listener = args[0] as ConnectionStateListener
        }
        1 * taskScheduler.schedule(_ as Runnable, _ as Trigger) >> {
            args ->
                reconciliationTask = args[0] as Runnable
                return scheduledFuture
        }
        1 * meterRegistry.gauge(AgentRoutingServiceCuratorDiscoveryImpl.CONNECTED_AGENTS_GAUGE_NAME, _, _, _)
        1 * meterRegistry.gaugeMapSize(AgentRoutingServiceCuratorDiscoveryImpl.REGISTERED_AGENTS_GAUGE_NAME, _, _)
        listener != null
        reconciliationTask != null

        when:
        agentRoutingService.handleClientConnected(jobId)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_CONNECTED_COUNTER_NAME, _) >> counter
        1 * counter.increment()

        when:
        reconciliationTask.run()

        then:
        1 * serviceDiscovery.registerService(_ as ServiceInstance) >> {
            throw new RuntimeException("...")
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        0 * serviceDiscovery.updateService(_ as ServiceInstance)

        when:
        reconciliationTask.run()

        then:
        1 * serviceDiscovery.registerService(_ as ServiceInstance) >> {
            args ->
                serviceInstanceJob = args[0] as ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent>
                return
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        serviceInstanceJob != null
        1 * serviceDiscovery.updateService(_)
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REFRESH_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)

        when:
        reconciliationTask.run()

        then:
        0 * serviceDiscovery.registerService(_ as ServiceInstance)
        0 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * serviceDiscovery.updateService(_ as ServiceInstance) >> {
            throw new KeeperException.NoNodeException("...")
        }
        1 * serviceDiscovery.registerService(_ as ServiceInstance)
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REFRESH_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)

        when:
        reconciliationTask.run()

        then:
        0 * serviceDiscovery.registerService(_ as ServiceInstance)
        0 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * serviceDiscovery.updateService(_ as ServiceInstance) >> {
            throw new KeeperException.NoNodeException("...")
        }
        1 * serviceDiscovery.registerService(_ as ServiceInstance) >> {
            throw new RuntimeException("...")
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REFRESH_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)

        when:
        reconciliationTask.run()

        then:
        0 * serviceDiscovery.registerService(_ as ServiceInstance)
        0 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * serviceDiscovery.updateService(_ as ServiceInstance) >> {
            throw new RuntimeException("...")
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REFRESH_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)

        when:
        agentRoutingService.handleClientDisconnected(jobId)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_DISCONNECTED_COUNTER_NAME, _) >> counter
        1 * counter.increment()

        when:
        reconciliationTask.run()

        then:
        1 * serviceDiscovery.unregisterService(serviceInstanceJob) >> {
            throw new RuntimeException("...")
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_UNREGISTERED_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        0 * serviceDiscovery.updateService(_ as ServiceInstance)

        when:
        reconciliationTask.run()

        then:
        1 * serviceDiscovery.unregisterService(serviceInstanceJob)
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_UNREGISTERED_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        0 * serviceDiscovery.updateService(_)
    }

    def "Query locally connected agents"() {
        setup:
        String jobId1 = UUID.randomUUID().toString() // Connected locally and registered
        String jobId2 = UUID.randomUUID().toString() // Connected locally and failed registration
        String jobId3 = UUID.randomUUID().toString() // Connected locally and not registered yet

        ConnectionStateListener listener
        Runnable reconciliationTask

        when:
        AgentRoutingService agentRoutingService = new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableConnectionState,
            meterRegistry
        )

        then:
        1 * genieHostInfo.getHostname() >> localHostname
        1 * listenableConnectionState.addListener(_ as ConnectionStateListener) >> {
            args ->
                listener = args[0] as ConnectionStateListener
        }
        1 * taskScheduler.schedule(_ as Runnable, _ as Trigger) >> {
            args ->
                reconciliationTask = args[0] as Runnable
                return scheduledFuture
        }
        1 * meterRegistry.gauge(AgentRoutingServiceCuratorDiscoveryImpl.CONNECTED_AGENTS_GAUGE_NAME, _, _, _)
        1 * meterRegistry.gaugeMapSize(AgentRoutingServiceCuratorDiscoveryImpl.REGISTERED_AGENTS_GAUGE_NAME, _, _)
        listener != null
        reconciliationTask != null

        when:
        agentRoutingService.handleClientConnected(jobId1)
        agentRoutingService.handleClientConnected(jobId2)
        reconciliationTask.run()

        then:
        2 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_CONNECTED_COUNTER_NAME, _) >> counter
        2 * counter.increment()
        2 * serviceDiscovery.registerService(_ as ServiceInstance) >> {
            args ->
                ServiceInstance serviceInstanceJob = args[0] as ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent>
                if (jobId2 == serviceInstanceJob.getId()) {
                    throw new RuntimeException("...")
                }
                return
        }
        2 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        2 * timer.record(_, TimeUnit.NANOSECONDS)


        when:
        agentRoutingService.handleClientConnected(jobId3)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_CONNECTED_COUNTER_NAME, _) >> counter
        1 * counter.increment()

        agentRoutingService.isAgentConnected(jobId1)
        agentRoutingService.isAgentConnected(jobId2)
        agentRoutingService.isAgentConnected(jobId3)
        agentRoutingService.isAgentConnectionLocal(jobId1)
        agentRoutingService.isAgentConnectionLocal(jobId2)
        agentRoutingService.isAgentConnectionLocal(jobId3)
        agentRoutingService.getHostnameForAgentConnection(jobId1).orElse(null) == localHostname
        agentRoutingService.getHostnameForAgentConnection(jobId2).orElse(null) == localHostname
        agentRoutingService.getHostnameForAgentConnection(jobId3).orElse(null) == localHostname

        when:
        agentRoutingService.handleClientDisconnected(jobId1)
        agentRoutingService.handleClientDisconnected(jobId2)
        agentRoutingService.handleClientDisconnected(jobId3)

        then:
        3 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_DISCONNECTED_COUNTER_NAME, _) >> counter
        3 * counter.increment()

        !agentRoutingService.isAgentConnectionLocal(jobId1)
        !agentRoutingService.isAgentConnectionLocal(jobId2)
        !agentRoutingService.isAgentConnectionLocal(jobId3)

        when:
        agentRoutingService.isAgentConnected(jobId1)
        agentRoutingService.isAgentConnected(jobId2)
        agentRoutingService.isAgentConnected(jobId3)

        then:
        3 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_LOOKUP_TIMER_NAME, _) >> timer
        3 * timer.record(_, TimeUnit.NANOSECONDS)
        1 * serviceDiscovery.queryForInstance(AgentRoutingServiceCuratorDiscoveryImpl.SERVICE_NAME, jobId1) >> null
        1 * serviceDiscovery.queryForInstance(AgentRoutingServiceCuratorDiscoveryImpl.SERVICE_NAME, jobId2) >> null
        1 * serviceDiscovery.queryForInstance(AgentRoutingServiceCuratorDiscoveryImpl.SERVICE_NAME, jobId3) >> null
    }

    def "Query remote connected agents"() {
        setup:
        String remoteHostname = UUID.randomUUID().toString()
        String jobId1 = UUID.randomUUID().toString() // Connected to remote node
        String jobId2 = UUID.randomUUID().toString() // Not connected
        String jobId3 = UUID.randomUUID().toString() // Throw error during lookup

        ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceInstanceJob1 = Mock(ServiceInstance)

        when:
        AgentRoutingService agentRoutingService = new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableConnectionState,
            meterRegistry
        )

        then:
        1 * genieHostInfo.getHostname() >> localHostname
        1 * listenableConnectionState.addListener(_ as ConnectionStateListener)
        1 * taskScheduler.schedule(_ as Runnable, _ as Trigger)
        1 * meterRegistry.gauge(AgentRoutingServiceCuratorDiscoveryImpl.CONNECTED_AGENTS_GAUGE_NAME, _, _, _)
        1 * meterRegistry.gaugeMapSize(AgentRoutingServiceCuratorDiscoveryImpl.REGISTERED_AGENTS_GAUGE_NAME, _, _)

        when:
        String hostnameJob1 = agentRoutingService.getHostnameForAgentConnection(jobId1).orElse(null)
        String hostnameJob2 = agentRoutingService.getHostnameForAgentConnection(jobId2).orElse(null)
        String hostnameJob3 = agentRoutingService.getHostnameForAgentConnection(jobId3).orElse(null)

        then:
        3 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_LOOKUP_TIMER_NAME, _) >> timer
        1 * serviceDiscovery.queryForInstance(AgentRoutingServiceCuratorDiscoveryImpl.SERVICE_NAME, jobId1) >> serviceInstanceJob1
        1 * serviceInstanceJob1.getAddress() >> remoteHostname
        1 * serviceDiscovery.queryForInstance(AgentRoutingServiceCuratorDiscoveryImpl.SERVICE_NAME, jobId2) >> null
        1 * serviceDiscovery.queryForInstance(AgentRoutingServiceCuratorDiscoveryImpl.SERVICE_NAME, jobId3) >> {
            throw new RuntimeException("...")
        }
        3 * timer.record(_, TimeUnit.NANOSECONDS)
        hostnameJob1 == remoteHostname
        hostnameJob2 == null
        hostnameJob3 == null
    }

    def "React to Zookeeper connection state"() {
        setup:
        CuratorFramework client = Mock(CuratorFramework)
        Runnable reconciliationTask
        ConnectionStateListener listener
        String jobId = UUID.randomUUID().toString()
        ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceInstance1
        ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceInstance2

        when:
        AgentRoutingService agentRoutingService = new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableConnectionState,
            meterRegistry
        )

        then:
        1 * genieHostInfo.getHostname() >> localHostname
        1 * listenableConnectionState.addListener(_ as ConnectionStateListener) >> {
            args ->
                listener = args[0] as ConnectionStateListener
        }
        1 * taskScheduler.schedule(_ as Runnable, _ as Trigger) >> {
            args ->
                reconciliationTask = args[0] as Runnable
                return scheduledFuture
        }
        1 * meterRegistry.gauge(AgentRoutingServiceCuratorDiscoveryImpl.CONNECTED_AGENTS_GAUGE_NAME, _, _, _)
        1 * meterRegistry.gaugeMapSize(AgentRoutingServiceCuratorDiscoveryImpl.REGISTERED_AGENTS_GAUGE_NAME, _, _)
        listener != null
        reconciliationTask != null

        when:
        listener.stateChanged(client, ConnectionState.SUSPENDED)
        listener.stateChanged(client, ConnectionState.RECONNECTED)

        then:
        2 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _ as Set<Tag>) >> counter
        2 * counter.increment()

        when:
        listener.stateChanged(client, ConnectionState.CONNECTED)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _ as Set<Tag>) >> counter
        1 * counter.increment()
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant)

        when:
        agentRoutingService.handleClientConnected(jobId)
        reconciliationTask.run()

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_CONNECTED_COUNTER_NAME, _) >> counter
        1 * counter.increment()
        serviceDiscovery.registerService(_ as ServiceInstance) >> {
            args ->
                serviceInstance1 = args[0] as ServiceInstance
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        serviceInstance1 != null
        serviceInstance1.getId() == jobId
        serviceInstance1.getAddress() == localHostname

        when:
        listener.stateChanged(client, ConnectionState.LOST)
        reconciliationTask.run()

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _ as Set<Tag>) >> counter
        1 * counter.increment()
        1 * serviceDiscovery.registerService(_ as ServiceInstance) >> {
            args ->
                serviceInstance2 = args[0] as ServiceInstance
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * timer.record(_, TimeUnit.NANOSECONDS)
        serviceInstance2 != null
        serviceInstance2.getId() == jobId
        serviceInstance2.getAddress() == localHostname
        serviceInstance2.getPayload() == serviceInstance1.getPayload()
    }
}
