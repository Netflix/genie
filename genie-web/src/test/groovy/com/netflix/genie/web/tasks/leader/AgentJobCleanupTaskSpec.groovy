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
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.exceptions.GenieException
import com.netflix.genie.web.agent.services.AgentRoutingService
import com.netflix.genie.web.data.services.DataServices
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.properties.AgentCleanupProperties
import com.netflix.genie.web.tasks.GenieTaskScheduleType
import com.netflix.genie.web.util.MetricsUtils
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import spock.lang.Specification

@SuppressWarnings("GroovyAccessibility")
class AgentJobCleanupTaskSpec extends Specification {

    AgentJobCleanupTask task
    PersistenceService persistenceService
    AgentCleanupProperties taskProperties
    MeterRegistry registry
    Counter counter
    AgentRoutingService agentRoutingService

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

    def "Run"() {

        def e = new GenieException(500, "...")

        when:
        GenieTaskScheduleType scheduleType = task.getScheduleType()
        long period = task.getFixedRate()

        then:
        scheduleType == GenieTaskScheduleType.FIXED_RATE
        period == 1000
        1 * taskProperties.getRefreshInterval() >> 1000

        when:
        task.run()

        then:
        1 * persistenceService.getActiveAgentJobs() >> Sets.newHashSet("j1", "j2", "j3", "j4")
        1 * agentRoutingService.isAgentConnected("j1") >> false
        1 * agentRoutingService.isAgentConnected("j2") >> false
        1 * agentRoutingService.isAgentConnected("j3") >> true
        1 * agentRoutingService.isAgentConnected("j4") >> true
        0 * taskProperties.getTimeLimit()

        when:
        task.run()

        then:
        1 * persistenceService.getActiveAgentJobs() >> Sets.newHashSet("j1", "j2", "j3", "j4")
        4 * agentRoutingService.isAgentConnected(_) >> false
        2 * taskProperties.getTimeLimit() >> -1 // Make sure they look expired
        1 * persistenceService.setJobCompletionInformation("j1", -1, JobStatus.FAILED, AgentJobCleanupTask.STATUS_MESSAGE, null, null)
        1 * persistenceService.setJobCompletionInformation("j2", -1, JobStatus.FAILED, AgentJobCleanupTask.STATUS_MESSAGE, null, null) >> {
            throw e
        }
        1 * registry.counter(AgentJobCleanupTask.TERMINATED_COUNTER_METRIC_NAME, MetricsUtils.newSuccessTagsSet()) >> counter
        1 * registry.counter(AgentJobCleanupTask.TERMINATED_COUNTER_METRIC_NAME, MetricsUtils.newFailureTagsSetForException(e)) >> counter
        2 * counter.increment()

        when:
        task.run()

        then:
        1 * persistenceService.getActiveAgentJobs() >> Sets.newHashSet("j3", "j4")
        1 * agentRoutingService.isAgentConnected("j3") >> false
        1 * agentRoutingService.isAgentConnected("j4") >> true
        1 * taskProperties.getTimeLimit() >> 10_000 // Make sure it's not expired
        0 * persistenceService._

        when:
        task.cleanup()

        then:
        noExceptionThrown()
    }
}
