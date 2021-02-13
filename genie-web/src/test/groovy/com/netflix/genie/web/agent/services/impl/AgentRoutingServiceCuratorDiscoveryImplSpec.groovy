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
import com.netflix.genie.web.properties.AgentRoutingServiceProperties
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.listen.Listenable
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.curator.x.discovery.ServiceDiscovery
import org.apache.curator.x.discovery.ServiceInstance
import org.apache.zookeeper.KeeperException
import org.springframework.scheduling.TaskScheduler
import spock.lang.Specification

import java.time.Instant
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ThreadFactory

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
    ThreadFactory threadFactory
    CuratorFramework curatorClient
    AgentRoutingServiceProperties serviceProperties = new AgentRoutingServiceProperties()
    Thread registrationThread

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
        this.threadFactory = Mock(ThreadFactory)
        this.curatorClient = Mock(CuratorFramework)
        this.registrationThread = Mock(Thread)
    }

    def "Handle Zookeeper connection state changes"() {
        setup:
        Thread t1 = Mock(Thread)
        Thread t2 = Mock(Thread)
        ConnectionStateListener listener

        when:
        new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableConnectionState,
            meterRegistry,
            serviceProperties,
            threadFactory
        )

        then:
        1 * genieHostInfo.getHostname() >> this.localHostname
        1 * listenableConnectionState.addListener(_ as ConnectionStateListener) >> {
            ConnectionStateListener l ->
                listener = l;
        }
        1 * threadFactory.newThread(_ as Runnable) >> registrationThread
        listener != null

        when:
        listener.stateChanged(curatorClient, ConnectionState.CONNECTED)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _) >> counter
        1 * registrationThread.interrupt()
        1 * threadFactory.newThread(_) >> t1
        1 * t1.start()
        0 * curatorClient._

        when:
        listener.stateChanged(curatorClient, ConnectionState.SUSPENDED)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _) >> counter
        1 * t1.interrupt()
        0 * curatorClient._

        when:
        listener.stateChanged(curatorClient, ConnectionState.LOST)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _) >> counter
        0 * t1._
        0 * curatorClient._

        when:
        listener.stateChanged(curatorClient, ConnectionState.CONNECTED)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _) >> counter
        1 * threadFactory.newThread(_) >> t2
        1 * t2.start()
        0 * curatorClient._

        when:
        listener.stateChanged(curatorClient, ConnectionState.LOST)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _) >> counter
        1 * t2.interrupt()
        0 * curatorClient._
    }

    def "Handle agent connections when not connected to Zookeeper"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        ConnectionStateListener listener

        when:
        AgentRoutingService agentRoutingService = new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableConnectionState,
            meterRegistry,
            serviceProperties,
            threadFactory
        )

        then:
        1 * genieHostInfo.getHostname() >> this.localHostname
        1 * listenableConnectionState.addListener(_ as ConnectionStateListener) >> {
            ConnectionStateListener l ->
                listener = l
        }
        1 * threadFactory.newThread(_ as Runnable) >> registrationThread
        listener != null

        when:
        agentRoutingService.handleClientConnected(jobId)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_CONNECTED_COUNTER_NAME) >> counter
        agentRoutingService.isAgentConnectionLocal(jobId)
        agentRoutingService.isAgentConnected(jobId)
        agentRoutingService.getHostnameForAgentConnection(jobId).get() == localHostname


    }

    def "Lookups"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        Optional<String> hostname
        ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceInstance = Mock(ServiceInstance)

        when:
        AgentRoutingService agentRoutingService = new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableConnectionState,
            meterRegistry,
            serviceProperties,
            threadFactory
        )

        then:
        1 * genieHostInfo.getHostname() >> this.localHostname
        1 * listenableConnectionState.addListener(_ as ConnectionStateListener)
        1 * threadFactory.newThread(_ as Runnable) >> registrationThread

        when:
        hostname = agentRoutingService.getHostnameForAgentConnection(jobId)

        then:
        1 * serviceDiscovery.queryForInstance(_, jobId) >> serviceInstance
        1 * serviceInstance.getAddress() >> "xyz.genie.com"
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_LOOKUP_TIMER_NAME, _) >> timer
        hostname.orElse(null) == "xyz.genie.com"

        when:
        hostname = agentRoutingService.getHostnameForAgentConnection(jobId)

        then:
        1 * serviceDiscovery.queryForInstance(_, jobId) >> null
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_LOOKUP_TIMER_NAME, _) >> timer
        !hostname.isPresent()

        when:
        hostname = agentRoutingService.getHostnameForAgentConnection(jobId)

        then:
        1 * serviceDiscovery.queryForInstance(_, jobId) >> { throw new KeeperException.SessionMovedException() }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_LOOKUP_TIMER_NAME, _) >> timer
        !hostname.isPresent()
    }

    def "Expected connection lifecycle without errors"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        Runnable refreshTask
        ConnectionStateListener listener
        ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceInstance

        when:
        AgentRoutingService agentRoutingService = new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableConnectionState,
            meterRegistry,
            serviceProperties,
            threadFactory
        )

        then:
        1 * genieHostInfo.getHostname() >> this.localHostname
        1 * listenableConnectionState.addListener(_ as ConnectionStateListener) >> {
            ConnectionStateListener l ->
                listener = l
        }
        1 * threadFactory.newThread(_ as Runnable) >> registrationThread
        listener != null

        when:
        agentRoutingService.handleClientConnected(jobId)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_CONNECTED_COUNTER_NAME) >> counter

        when:
        agentRoutingService.processNextRegistrationMutation()

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> {
            Runnable r, Instant i ->
                refreshTask = r
                return null
        }
        1 * serviceDiscovery.registerService(_ as ServiceInstance) >> {
            ServiceInstance si ->
                serviceInstance = si
        }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        refreshTask != null
        serviceInstance != null
        serviceInstance.getAddress() == localHostname
        serviceInstance.getId() == jobId

        when:
        refreshTask.run()

        then:
        noExceptionThrown()

        when:
        agentRoutingService.processNextRegistrationMutation()

        then:
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant)
        1 * serviceDiscovery.updateService(serviceInstance)
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REFRESH_TIMER_NAME, _) >> timer

        when:
        agentRoutingService.handleClientDisconnected(jobId)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_DISCONNECTED_COUNTER_NAME) >> counter

        when:
        agentRoutingService.processNextRegistrationMutation()

        then:
        1 * serviceDiscovery.unregisterService(serviceInstance)
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_UNREGISTERED_TIMER_NAME, _) >> timer
    }


    def "Registration errors"() {
        setup:
        String jobId = UUID.randomUUID().toString()
        Runnable refreshTask

        when:
        AgentRoutingService agentRoutingService = new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableConnectionState,
            meterRegistry,
            serviceProperties,
            threadFactory
        )

        then:
        1 * genieHostInfo.getHostname() >> this.localHostname
        1 * listenableConnectionState.addListener(_ as ConnectionStateListener)
        1 * threadFactory.newThread(_ as Runnable) >> registrationThread

        when: "Agent connects"
        agentRoutingService.handleClientConnected(jobId)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_CONNECTED_COUNTER_NAME) >> counter

        when:
        agentRoutingService.processNextRegistrationMutation()

        then: "Registration is attempted, but thread is interrupted, mutation is back in the queue"
        1 * serviceDiscovery.registerService(_ as ServiceInstance) >> { throw new InterruptedException() }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        thrown(InterruptedException)

        when:
        agentRoutingService.processNextRegistrationMutation()

        then: "Registration is attempted again, error is encountered. Registration is scheduled for refresh"
        1 * serviceDiscovery.registerService(_ as ServiceInstance) >> { throw new RuntimeException() }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> {
            Runnable r, Instant i ->
                refreshTask = r
                return null
        }

        when:
        refreshTask.run()
        agentRoutingService.processNextRegistrationMutation()

        then: "Registration refresh is attempted, but service was never registered"
        1 * serviceDiscovery.registerService(_ as ServiceInstance)
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> {
            Runnable r, Instant i ->
                refreshTask = r
                return null
        }

        when:
        refreshTask.run()
        agentRoutingService.processNextRegistrationMutation()

        then: "Registration refresh is attempted, but node does not exist"
        1 * serviceDiscovery.updateService(_ as ServiceInstance) >> { throw new KeeperException.NoNodeException() }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REFRESH_TIMER_NAME, _) >> timer
        1 * serviceDiscovery.registerService(_ as ServiceInstance) >> { throw new RuntimeException() }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REGISTERED_TIMER_NAME, _) >> timer
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> {
            Runnable r, Instant i ->
                refreshTask = r
                return null
        }

        when:
        refreshTask.run()
        agentRoutingService.processNextRegistrationMutation()

        then: "Registration refresh is attempted, but thread is interrupted, mutation goes back in queue"
        1 * serviceDiscovery.updateService(_ as ServiceInstance) >> { throw new InterruptedException() }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REFRESH_TIMER_NAME, _) >> timer
        thrown(InterruptedException)

        when:
        refreshTask.run()
        agentRoutingService.processNextRegistrationMutation()

        then: "Registration refresh is attempted, but node does not exist"
        1 * serviceDiscovery.updateService(_ as ServiceInstance) >> { throw new RuntimeException() }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_REFRESH_TIMER_NAME, _) >> timer
        1 * taskScheduler.schedule(_ as Runnable, _ as Instant) >> {
            Runnable r, Instant i ->
                refreshTask = r
                return null
        }

        when:
        refreshTask.run()
        agentRoutingService.handleClientDisconnected(jobId)

        then: "Disconnection and refresh are queued, the former is processed first"
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_DISCONNECTED_COUNTER_NAME) >> counter

        when:
        agentRoutingService.processNextRegistrationMutation()

        then: "Unregistration is attempted, but thread is interrupted"
        1 * serviceDiscovery.unregisterService(_ as ServiceInstance) >> { throw new InterruptedException() }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_UNREGISTERED_TIMER_NAME, _) >> timer
        thrown(InterruptedException)

        when:
        agentRoutingService.processNextRegistrationMutation()

        then: "Unregistration is attempted, but an error is encountered"
        1 * serviceDiscovery.unregisterService(_ as ServiceInstance) >> { throw new RuntimeException() }
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_UNREGISTERED_TIMER_NAME, _) >> timer

        when:
        agentRoutingService.processNextRegistrationMutation()

        then: "Unregistration is attempted, but an error is encountered"
        1 * serviceDiscovery.unregisterService(_ as ServiceInstance)
        1 * meterRegistry.timer(AgentRoutingServiceCuratorDiscoveryImpl.AGENT_UNREGISTERED_TIMER_NAME, _) >> timer

        when:
        agentRoutingService.processNextRegistrationMutation()

        then: "Previous refresh does nothing"
        0 * serviceDiscovery._
        0 * taskScheduler.schedule(_, _)
    }

    def "???"() {
        ConnectionStateListener listener
        when:
        AgentRoutingService agentRoutingService = new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableConnectionState,
            meterRegistry,
            serviceProperties,
            threadFactory
        )

        then:
        1 * genieHostInfo.getHostname() >> this.localHostname
        1 * listenableConnectionState.addListener(_ as ConnectionStateListener) >> {
            ConnectionStateListener l ->
                listener = l
        }
        1 * threadFactory.newThread(_ as Runnable) >> registrationThread
        listener != null

        when:
        listener.stateChanged(curatorClient, ConnectionState.CONNECTED)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _) >> counter
        1 * threadFactory.newThread(_ as Runnable) >> new Thread(
            {
                println("running")
            }
        )

        when:
        listener.stateChanged(curatorClient, ConnectionState.SUSPENDED)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _) >> counter

        when:
        listener.stateChanged(curatorClient, ConnectionState.RECONNECTED)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _) >> counter
        1 * threadFactory.newThread(_ as Runnable) >> new Thread(
            {
                println("running")
            }
        )

        when:
        listener.stateChanged(curatorClient, ConnectionState.LOST)

        then:
        1 * meterRegistry.counter(AgentRoutingServiceCuratorDiscoveryImpl.ZOOKEEPER_SESSION_STATE_COUNTER_NAME, _) >> counter

    }

    def "Mutations order prioritizes creation and deletions"() {

        Queue<AgentRoutingServiceCuratorDiscoveryImpl.RegisterMutation> queue = new PriorityBlockingQueue<>()
        queue.add(AgentRoutingServiceCuratorDiscoveryImpl.RegisterMutation.update("j1"),)
        queue.add(AgentRoutingServiceCuratorDiscoveryImpl.RegisterMutation.refresh("j2"),)
        queue.add(AgentRoutingServiceCuratorDiscoveryImpl.RegisterMutation.update("j3"),)
        queue.add(AgentRoutingServiceCuratorDiscoveryImpl.RegisterMutation.refresh("j4"))

        expect:
        queue.take().getJobId() == "j1"
        queue.take().getJobId() == "j3"
        queue.take().getJobId() == "j2"
        queue.take().getJobId() == "j4"
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
}
