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
package com.netflix.genie.web.tasks.leader

import com.google.common.collect.Sets
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException
import com.netflix.genie.web.agent.services.AgentRoutingService
import com.netflix.genie.web.data.services.DataServices
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.exceptions.checked.NotFoundException
import com.netflix.genie.web.properties.AgentCleanupProperties
import com.netflix.genie.web.tasks.GenieTaskScheduleType
import com.netflix.genie.web.util.MetricsUtils
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import spock.lang.Specification

import java.time.Duration

@SuppressWarnings("GroovyAccessibility")
class AgentJobCleanupTaskSpec extends Specification {

    AgentJobCleanupTask task
    PersistenceService persistenceService
    AgentCleanupProperties taskProperties
    MeterRegistry registry
    Counter counter
    AgentRoutingService agentRoutingService
    Duration inTheFuture = Duration.ofHours(1)
    Duration inThePast = Duration.ofHours(-1)

    void setup() {
        this.persistenceService = Mock(PersistenceService)
        def dataServices = Mock(DataServices) {
            getPersistenceService() >> this.persistenceService
        }
        this.taskProperties = Mock(AgentCleanupProperties)
        this.registry = Mock(MeterRegistry)
        this.counter = Mock(Counter)
        this.agentRoutingService = Mock(AgentRoutingService)
        this.task = new AgentJobCleanupTask(
            dataServices,
            this.taskProperties,
            this.registry,
            agentRoutingService
        )
    }

    def "Task type and rate"() {
        when:
        GenieTaskScheduleType scheduleType = task.getScheduleType()
        long period = task.getFixedRate()

        then:
        scheduleType == GenieTaskScheduleType.FIXED_RATE
        period == 10_000
        1 * taskProperties.getRefreshInterval() >> Duration.ofSeconds(10)
    }

    def "Run"() {
        setup:
        Exception e = new GenieInvalidStatusException("...")

        when:
        task.run()

        then:
        1 * persistenceService.getActiveJobs() >> Sets.newHashSet(
            "j1", //Active status, connected
            "j2", // Accepted status, connected
            "j3", // Active status, disconnected
            "j4", // Accepted status, disconnected
            "j5", // Active status, disconnected (reconnects next iteration)
            "j6" // Accepted status, disconnected (reconnects next iteration)
        )
        1 * persistenceService.getUnclaimedJobs() >> Sets.newHashSet("j2", "j4", "j6")
        1 * agentRoutingService.isAgentConnected("j1") >> true
        1 * agentRoutingService.isAgentConnected("j2") >> true
        1 * agentRoutingService.isAgentConnected("j3") >> false
        1 * agentRoutingService.isAgentConnected("j4") >> false
        1 * agentRoutingService.isAgentConnected("j5") >> false
        1 * agentRoutingService.isAgentConnected("j6") >> false
        4 * taskProperties.getLaunchTimeLimit() >> inTheFuture
        4 * taskProperties.getReconnectTimeLimit() >> inTheFuture
        0 * persistenceService.getJobStatus(_)
        0 * persistenceService.getJobArchiveStatus(_)
        0 * registry.counter(_, _)

        when:
        task.run()

        then:
        1 * persistenceService.getActiveJobs() >> Sets.newHashSet(
            "j1", //Active status, connected
            "j2", // Accepted status, connected
            "j3", // Active status, disconnected
            "j4", // Accepted status, disconnected
            "j5", // Active status, just reconnected
            "j6" // Accepted status, just reconnected
        )
        1 * persistenceService.getUnclaimedJobs() >> Sets.newHashSet("j2", "j4", "j6")
        1 * agentRoutingService.isAgentConnected("j1") >> true
        1 * agentRoutingService.isAgentConnected("j2") >> true
        1 * agentRoutingService.isAgentConnected("j3") >> false
        1 * agentRoutingService.isAgentConnected("j4") >> false
        1 * agentRoutingService.isAgentConnected("j5") >> true
        1 * agentRoutingService.isAgentConnected("j6") >> true
        2 * taskProperties.getLaunchTimeLimit() >> inTheFuture
        2 * taskProperties.getReconnectTimeLimit() >> inTheFuture
        0 * persistenceService.getJobStatus(_)
        0 * persistenceService.getJobArchiveStatus(_)
        0 * registry.counter(_, _)

        when:
        task.run()

        then:
        1 * persistenceService.getActiveJobs() >> Sets.newHashSet(
            "j3", // Active status, disconnected
            "j4", // Accepted status, disconnected
        )
        1 * persistenceService.getUnclaimedJobs() >> Sets.newHashSet("j4", "j6")
        1 * agentRoutingService.isAgentConnected("j3") >> false
        1 * agentRoutingService.isAgentConnected("j4") >> false
        2 * taskProperties.getLaunchTimeLimit() >> inTheFuture
        2 * taskProperties.getReconnectTimeLimit() >> inThePast
        1 * persistenceService.getJobStatus("j3") >> JobStatus.INIT
        1 * persistenceService.getJobArchiveStatus("j3") >> ArchiveStatus.PENDING
        1 * persistenceService.updateJobStatus("j3", JobStatus.INIT, JobStatus.FAILED, AgentJobCleanupTask.AWOL_STATUS_MESSAGE)
        1 * persistenceService.updateJobArchiveStatus("j3", ArchiveStatus.UNKNOWN)
        1 * registry.counter(AgentJobCleanupTask.TERMINATED_COUNTER_METRIC_NAME, MetricsUtils.newSuccessTagsSet()) >> counter
        1 * counter.increment()

        when:
        task.run()

        then:
        1 * persistenceService.getActiveJobs() >> Sets.newHashSet(
            "j4", // Accepted status, disconnected
        )
        1 * persistenceService.getUnclaimedJobs() >> Sets.newHashSet("j4")
        1 * agentRoutingService.isAgentConnected("j4") >> false
        1 * taskProperties.getLaunchTimeLimit() >> inThePast
        1 * taskProperties.getReconnectTimeLimit() >> inTheFuture
        1 * persistenceService.getJobStatus("j4") >> JobStatus.ACCEPTED
        1 * persistenceService.getJobArchiveStatus("j4") >> ArchiveStatus.PENDING
        1 * persistenceService.updateJobArchiveStatus("j4", ArchiveStatus.FAILED)
        1 * persistenceService.updateJobStatus("j4", JobStatus.ACCEPTED, JobStatus.FAILED, AgentJobCleanupTask.NEVER_CLAIMED_STATUS_MESSAGE) >> {
            throw e
        }
        1 * registry.counter(AgentJobCleanupTask.TERMINATED_COUNTER_METRIC_NAME, MetricsUtils.newFailureTagsSetForException(e)) >> counter
        1 * counter.increment()

        when:
        task.cleanup()

        then:
        noExceptionThrown()

        when:
        task.run()

        then:
        1 * persistenceService.getActiveJobs() >> Sets.newHashSet(
            "j4", // Accepted status, disconnected
        )
        1 * persistenceService.getUnclaimedJobs() >> Sets.newHashSet("j4")
        1 * agentRoutingService.isAgentConnected("j4") >> false
        1 * taskProperties.getLaunchTimeLimit() >> inThePast
        1 * taskProperties.getReconnectTimeLimit() >> inTheFuture
        1 * persistenceService.getJobStatus("j4") >> {
            throw new NotFoundException("...")
        }
        1 * registry.counter(AgentJobCleanupTask.TERMINATED_COUNTER_METRIC_NAME, _) >> counter
        1 * counter.increment()

        when:
        task.cleanup()

        then:
        noExceptionThrown()
        when:
        task.run()

        then:
        1 * persistenceService.getActiveJobs() >> Sets.newHashSet(
            "j4", // Accepted status, disconnected
        )
        1 * persistenceService.getUnclaimedJobs() >> Sets.newHashSet("j4")
        1 * agentRoutingService.isAgentConnected("j4") >> false
        1 * taskProperties.getLaunchTimeLimit() >> inThePast
        1 * taskProperties.getReconnectTimeLimit() >> inTheFuture
        1 * persistenceService.getJobStatus("j4") >> JobStatus.ACCEPTED
        1 * persistenceService.getJobArchiveStatus("j4") >> ArchiveStatus.PENDING
        1 * persistenceService.updateJobArchiveStatus("j4", ArchiveStatus.FAILED)
        1 * persistenceService.updateJobStatus("j4", JobStatus.ACCEPTED, JobStatus.FAILED, AgentJobCleanupTask.NEVER_CLAIMED_STATUS_MESSAGE)
        1 * registry.counter(AgentJobCleanupTask.TERMINATED_COUNTER_METRIC_NAME, _) >> counter
        1 * counter.increment()

        when:
        task.cleanup()

        then:
        noExceptionThrown()
    }
}
