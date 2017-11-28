package com.netflix.genie.core.services.impl

import com.google.common.collect.Lists
import com.netflix.genie.common.dto.Application
import com.netflix.genie.common.dto.Cluster
import com.netflix.genie.common.dto.Command
import com.netflix.genie.common.dto.JobRequest
import com.netflix.genie.core.events.GenieEventBus
import com.netflix.genie.core.services.JobStateService
import com.netflix.genie.core.services.JobSubmitterService
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import org.springframework.scheduling.TaskScheduler
import spock.lang.Specification

/**
 * Test JobStateService
 *
 * @author amajumdar
 * @since 3.0.0
 */
class JobStateServiceImplSpec extends Specification {
    JobSubmitterService jobSubmitterService = Mock(JobSubmitterService)
    TaskScheduler scheduler = Mock(TaskScheduler)
    GenieEventBus genieEventBus = Mock(GenieEventBus)
    Registry registry = new DefaultRegistry()
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
