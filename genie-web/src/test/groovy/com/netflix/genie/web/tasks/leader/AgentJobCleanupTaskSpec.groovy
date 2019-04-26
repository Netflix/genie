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
import com.netflix.genie.web.properties.AgentCleanupProperties
import com.netflix.genie.web.services.JobPersistenceService
import com.netflix.genie.web.services.JobSearchService
import com.netflix.genie.web.tasks.GenieTaskScheduleType
import spock.lang.Specification

class AgentJobCleanupTaskSpec extends Specification {

    AgentJobCleanupTask task
    JobSearchService jobSearchService
    JobPersistenceService jobPersistenceService
    AgentCleanupProperties taskProperties

    void setup() {
        this.jobSearchService = Mock(JobSearchService)
        this.jobPersistenceService = Mock(JobPersistenceService)
        this.taskProperties = Mock(AgentCleanupProperties)

        this.task = new AgentJobCleanupTask(
            jobSearchService,
            jobPersistenceService,
            taskProperties
        )
    }

    def "Run"() {

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
        1 * jobSearchService.getActiveDisconnectedAgentJobs() >> Sets.newHashSet("j1", "j2")
        2 * taskProperties.getTimeLimit() >> -1 // Make sure it's past deadline next iteration

        when:
        task.run()

        then:
        1 * jobSearchService.getActiveDisconnectedAgentJobs() >> Sets.newHashSet("j1", "j2", "j3", "j4")
        2 * taskProperties.getTimeLimit() >> 1_000_000L // Make sure it not expiring
        1 * jobPersistenceService.setJobCompletionInformation("j1", -1, JobStatus.FAILED, AgentJobCleanupTask.STATUS_MESSAGE, null, null)
        1 * jobPersistenceService.setJobCompletionInformation("j2", -1, JobStatus.FAILED, AgentJobCleanupTask.STATUS_MESSAGE, null, null) >> { throw new GenieException(500, "...")}

        when:
        task.run()

        then:
        1 * jobSearchService.getActiveDisconnectedAgentJobs() >> Sets.newHashSet("j2", "j3")
        1 * jobPersistenceService.setJobCompletionInformation("j2", -1, JobStatus.FAILED, AgentJobCleanupTask.STATUS_MESSAGE, null, null)

        when:
        task.cleanup()

        then:
        noExceptionThrown()
    }
}
