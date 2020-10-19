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

import com.netflix.genie.web.agent.services.AgentRoutingService
import com.netflix.genie.web.properties.AgentConnectionTrackingServiceProperties
import org.springframework.boot.actuate.info.Info
import org.springframework.scheduling.TaskScheduler
import spock.lang.Specification

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ScheduledFuture
import java.util.function.Supplier

class AgentConnectionTrackingServiceImplSpec extends Specification {

    AgentConnectionTrackingServiceImpl service
    AgentRoutingService agentRoutingService
    TaskScheduler taskScheduler
    Supplier<Instant> timeSupplier
    AgentConnectionTrackingServiceProperties serviceProperties

    void setup() {
        this.serviceProperties = new AgentConnectionTrackingServiceProperties()
        this.agentRoutingService = Mock(AgentRoutingService)
        this.taskScheduler = Mock(TaskScheduler)
        this.timeSupplier = Mock(Supplier)
    }

    def "Cleanup with no connected agents"() {
        setup:
        Runnable cleanupTask

        when:
        this.service = new AgentConnectionTrackingServiceImpl(agentRoutingService, taskScheduler, serviceProperties)

        then:
        1 * taskScheduler.scheduleAtFixedRate(_ as Runnable, _ as Duration) >> {
            args ->
                cleanupTask = args[0] as Runnable
                return Mock(ScheduledFuture)
        }
        cleanupTask != null

        when:
        cleanupTask.run()

        then:
        0 * agentRoutingService._
    }

    def "2 agents"() {
        setup:
        Runnable cleanupTask
        String agent1streamId = "Agent-1-stream-X"
        String agent2streamId = "Agent-2-stream-Y"
        String agent1jobId = "Agent-1-job"
        String agent2jobId = "Agent-2-job"
        Instant currentTime

        when:
        this.service = new AgentConnectionTrackingServiceImpl(agentRoutingService, taskScheduler, serviceProperties, timeSupplier)

        then:
        1 * taskScheduler.scheduleAtFixedRate(_ as Runnable, _ as Duration) >> {
            args ->
                cleanupTask = args[0] as Runnable
                return Mock(ScheduledFuture)
        }
        cleanupTask != null

        when: "Receive first heartbeat from 2 different agents"
        currentTime = Instant.now()
        service.notifyHeartbeat(agent1streamId, agent1jobId)
        service.notifyHeartbeat(agent2streamId, agent2jobId)

        then: "Routing service notified: 2 new agents"
        2 * timeSupplier.get() >> { return currentTime }
        1 * agentRoutingService.handleClientConnected(agent1jobId)
        1 * agentRoutingService.handleClientConnected(agent2jobId)

        expect:
        service.getConnectedAgentsCount() == 2

        when: "Cleanup runs"
        currentTime = currentTime.plusMillis(100)
        cleanupTask.run()

        then: "Both agents are still alive"
        1 * timeSupplier.get() >> { return currentTime }
        0 * agentRoutingService._

        expect:
        service.getConnectedAgentsCount() == 2

        when: "First agent renews heartbeat"
        currentTime = currentTime.plusMillis(3_000)
        service.notifyHeartbeat(agent1streamId, agent1jobId)

        then: "Record is updated"
        1 * timeSupplier.get() >> { return currentTime }

        when: "Cleanup runs"
        currentTime = currentTime.plusMillis(10_000)
        cleanupTask.run()

        then: "Routing service notified: agent 2 is gone"
        1 * timeSupplier.get() >> { return currentTime }
        1 * agentRoutingService.handleClientDisconnected(agent2jobId)

        expect:
        service.getConnectedAgentsCount() == 1

        when: "First agent disconnects"
        service.notifyDisconnected(agent1streamId, agent1jobId)

        then: "Routing service notified: agent 1 is gone"
        1 * agentRoutingService.handleClientDisconnected(agent1jobId)
        0 * timeSupplier._

        expect:
        service.getConnectedAgentsCount() == 0

        when: "Cleanup runs"
        currentTime = currentTime.plusMillis(1000)
        cleanupTask.run()

        then:
        1 * timeSupplier.get() >> { return currentTime }
        0 * agentRoutingService._
    }

    def "1 agent, multiple streams"() {
        setup:
        Runnable cleanupTask
        String jobId = UUID.randomUUID().toString()
        String stream1 = UUID.randomUUID().toString()
        String stream2 = UUID.randomUUID().toString()
        Instant currentTime = Instant.now()

        when:
        this.service = new AgentConnectionTrackingServiceImpl(agentRoutingService, taskScheduler, serviceProperties, timeSupplier)

        then:
        1 * taskScheduler.scheduleAtFixedRate(_ as Runnable, _ as Duration) >> {
            args ->
                cleanupTask = args[0] as Runnable
                return Mock(ScheduledFuture)
        }
        cleanupTask != null

        when: "Agent connects"
        currentTime = Instant.now()
        service.notifyHeartbeat(stream1, jobId)

        then: "Routing service notified"
        1 * timeSupplier.get() >> { return currentTime }
        1 * agentRoutingService.handleClientConnected(jobId)

        when: "Agent starts a second connection"
        currentTime = currentTime.plusMillis(1000)
        service.notifyHeartbeat(stream2, jobId)

        then: "Routing service not notified"
        1 * timeSupplier.get() >> { return currentTime }
        0 * agentRoutingService._

        when: "Agent renews second connection"
        currentTime = currentTime.plusMillis(100)
        service.notifyHeartbeat(stream2, jobId)

        then:
        1 * timeSupplier.get() >> { return currentTime }
        0 * agentRoutingService._

        when: "Cleanup runs after stream 1 is expired"
        currentTime = currentTime.plusMillis(10_000)
        cleanupTask.run()

        then: "Agent not removed from routing"
        1 * timeSupplier.get() >> { return currentTime }
        0 * agentRoutingService._

        when: "First connection disappears"
        service.notifyDisconnected(stream1, jobId)

        then:
        0 * agentRoutingService._

        when: "Second connection disconnected"
        currentTime = currentTime.plusMillis(1000)
        service.notifyDisconnected(stream2, jobId)

        then:
        1 * agentRoutingService.handleClientDisconnected(jobId)
    }

    def "Contribute info detail"() {
        setup:
        String agent1streamId = UUID.randomUUID().toString()
        String agent2streamId = UUID.randomUUID().toString()
        String agent1jobId = UUID.randomUUID().toString()
        String agent2jobId = UUID.randomUUID().toString()
        List<String> connectedAgents
        Info.Builder infoBuilder = Mock(Info.Builder)

        when:
        this.service = new AgentConnectionTrackingServiceImpl(agentRoutingService, taskScheduler, serviceProperties)

        then:
        1 * taskScheduler.scheduleAtFixedRate(_ as Runnable, _ as Duration) >> Mock(ScheduledFuture)

        when:
        connectedAgents = null
        service.contribute(infoBuilder)

        then:
        1 * infoBuilder.withDetail("connectedAgents", _ as List<String>) >> {
            args ->
                connectedAgents = args[1] as List<String>
                return infoBuilder
        }
        connectedAgents != null
        connectedAgents.size() == 0

        when:
        service.notifyHeartbeat(agent1streamId, agent1jobId)
        service.notifyHeartbeat(agent2streamId, agent2jobId)

        then:
        noExceptionThrown()

        when:
        connectedAgents = null
        service.contribute(infoBuilder)

        then:
        1 * infoBuilder.withDetail("connectedAgents", _ as List<String>) >> {
            args ->
                connectedAgents = args[1] as List<String>
                return infoBuilder
        }
        connectedAgents != null
        connectedAgents.size() == 2
        connectedAgents.contains(agent1jobId)
        connectedAgents.contains(agent2jobId)
    }
}
