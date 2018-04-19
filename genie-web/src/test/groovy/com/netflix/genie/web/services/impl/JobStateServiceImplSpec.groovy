/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.services.impl

import com.google.common.collect.Lists
import com.netflix.genie.common.dto.JobRequest
import com.netflix.genie.common.internal.dto.v4.Application
import com.netflix.genie.common.internal.dto.v4.Cluster
import com.netflix.genie.common.internal.dto.v4.Command
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.events.GenieEventBus
import com.netflix.genie.web.services.JobStateService
import com.netflix.genie.web.services.JobSubmitterService
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.experimental.categories.Category
import org.springframework.scheduling.TaskScheduler
import spock.lang.Specification

/**
 * Test JobStateService
 *
 * @author amajumdar
 * @since 3.0.0
 */
@Category(UnitTest.class)
class JobStateServiceImplSpec extends Specification {
    JobSubmitterService jobSubmitterService = Mock(JobSubmitterService)
    TaskScheduler scheduler = Mock(TaskScheduler)
    GenieEventBus genieEventBus = Mock(GenieEventBus)
    MeterRegistry registry = new SimpleMeterRegistry()
    JobRequest jobRequest = Mock(JobRequest)
    Cluster cluster = Mock(Cluster)
    Command command = Mock(Command)
    List<Application> applications = Lists.newArrayList(Mock(Application))
    JobStateService jobStateService = new JobStateServiceImpl(jobSubmitterService, scheduler, genieEventBus, registry)
    String job1Id = "1"
    String job2Id = "2"
    int memory = 1024

    def testInit() {
        when:
        jobStateService.init(job1Id)
        then:
        jobStateService.jobExists(job1Id)
        jobStateService.getNumActiveJobs() == 0
        when:
        jobStateService.init(job1Id)
        jobStateService.init(job1Id)
        then:
        jobStateService.jobExists(job1Id)
        jobStateService.getNumActiveJobs() == 0
    }

    def testSchedule() {
        when:
        jobStateService.init(job1Id)
        jobStateService.schedule(job1Id, jobRequest, cluster, command, applications, memory)
        then:
        jobStateService.jobExists(job1Id)
        jobStateService.getNumActiveJobs() == 1
        jobStateService.getUsedMemory() == 1024
        when:
        jobStateService.init(job2Id)
        jobStateService.schedule(job2Id, jobRequest, cluster, command, applications, memory)
        then:
        jobStateService.jobExists(job1Id)
        jobStateService.jobExists(job2Id)
        jobStateService.getNumActiveJobs() == 2
        jobStateService.getUsedMemory() == 2048
        when:
        jobStateService.done(job2Id)
        jobStateService.schedule(job2Id, jobRequest, cluster, command, applications, memory)
        then:
        jobStateService.jobExists(job1Id)
        !jobStateService.jobExists(job2Id)
        jobStateService.getNumActiveJobs() == 1
        jobStateService.getUsedMemory() == 1024
    }

    def testDone() {
        when:
        jobStateService.init(job1Id)
        jobStateService.done(job1Id)
        then:
        !jobStateService.jobExists(job1Id)
        jobStateService.getNumActiveJobs() == 0
        jobStateService.getUsedMemory() == 0
    }
}
