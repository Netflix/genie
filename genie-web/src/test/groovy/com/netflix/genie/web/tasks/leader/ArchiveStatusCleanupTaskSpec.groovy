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
package com.netflix.genie.web.tasks.leader

import com.google.common.collect.Sets
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.web.agent.services.AgentRoutingService
import com.netflix.genie.web.data.services.DataServices
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.exceptions.checked.NotFoundException
import com.netflix.genie.web.properties.ArchiveStatusCleanupProperties
import com.netflix.genie.web.tasks.GenieTaskScheduleType
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification

import java.time.Instant

class ArchiveStatusCleanupTaskSpec extends Specification {

    DataServices dataServices
    AgentRoutingService agentRoutingService
    PersistenceService persistenceServiceMock
    ArchiveStatusCleanupProperties props
    MeterRegistry registry
    ArchiveStatusCleanupTask task

    void setup() {
        this.persistenceServiceMock = Mock(PersistenceService)
        this.dataServices = Mock(DataServices) {
            getPersistenceService() >> persistenceServiceMock
        }
        this.agentRoutingService = Mock(AgentRoutingService)
        this.props = new ArchiveStatusCleanupProperties()
        this.registry = new SimpleMeterRegistry()
        this.task = new ArchiveStatusCleanupTask(
            dataServices,
            agentRoutingService,
            props,
            registry
        )
    }

    def "Run"() {
        when:
        task.run()

        then:
        1 * persistenceServiceMock.getJobsWithStatusAndArchiveStatusUpdatedBefore(_ as Set, _ as Set, _ as Instant) >> {
            Set statuses, Set archiveStatuses, Instant threshold ->
                assert statuses == Sets.newHashSet(JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.KILLED, JobStatus.INVALID)
                assert archiveStatuses == Sets.newHashSet(ArchiveStatus.PENDING)
                assert threshold < Instant.now() - props.gracePeriod
                return Sets.newHashSet()
        }
        0 * agentRoutingService._
        0 * persistenceServiceMock.updateJobArchiveStatus(_, _)

        when:
        task.run()

        then:
        1 * persistenceServiceMock.getJobsWithStatusAndArchiveStatusUpdatedBefore(_, _, _) >> Sets.newHashSet("j1", "j2", "j3")

        then:
        1 * agentRoutingService.isAgentConnected("j1") >> true
        0 * persistenceServiceMock.updateJobArchiveStatus("j1", _)
        1 * agentRoutingService.isAgentConnected("j2") >> false
        1 * persistenceServiceMock.updateJobArchiveStatus("j2", ArchiveStatus.UNKNOWN) >> { throw new NotFoundException("...") }
        1 * agentRoutingService.isAgentConnected("j3") >> false
        1 * persistenceServiceMock.updateJobArchiveStatus("j3", ArchiveStatus.UNKNOWN)

        when:
        task.run()

        then:
        1 * persistenceServiceMock.getJobsWithStatusAndArchiveStatusUpdatedBefore(_, _, _) >> Sets.newHashSet("j4")

        then:
        1 * agentRoutingService.isAgentConnected("j4") >> { throw new RuntimeException("...") }
        0 * persistenceServiceMock.updateJobArchiveStatus("j4", ArchiveStatus.UNKNOWN)
        noExceptionThrown()
    }

    def "GetScheduleType and getFixedRate"() {
        expect:
        task.getScheduleType() == GenieTaskScheduleType.FIXED_RATE
        task.getFixedRate() == props.getCheckInterval().toMillis()
    }
}
