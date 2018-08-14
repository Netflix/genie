/*
 *
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.genie.agent.execution.statemachine.actions

import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException
import com.netflix.genie.agent.execution.exceptions.JobLaunchException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.services.LaunchJobService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class LaunchJobActionSpec extends Specification {
    String id
    ExecutionContext executionContext
    JobSpecification jobSpec
    Map<String, String> jobEnvironment
    LaunchJobAction action
    LaunchJobService launchJobService
    AgentJobService agentJobService
    Process process
    File jobRunDirectory
    List<String> jobCommandLine
    boolean interactive

    void setup() {
        this.id = UUID.randomUUID().toString()
        this.executionContext = Mock(ExecutionContext)
        this.jobSpec = Mock(JobSpecification)
        this.jobRunDirectory = Mock(File)
        this.jobEnvironment = Mock(Map)
        this.jobCommandLine = Mock(List)
        this.interactive = true
        this.launchJobService = Mock(LaunchJobService)
        this.agentJobService = Mock(AgentJobService)
        this.process = Mock(Process)
        this.action = new LaunchJobAction(executionContext, launchJobService, agentJobService)
    }

    void cleanup() {
    }

    def "Successful"() {
        JobStatus currentJobStatus = JobStatus.INIT

        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobRunDirectory
        1 * executionContext.getJobEnvironment() >> jobEnvironment
        1 * jobSpec.getCommandArgs() >> jobCommandLine
        1 * jobSpec.isInteractive() >> interactive
        1 * launchJobService.launchProcess(jobRunDirectory, jobEnvironment, jobCommandLine, interactive)
        1 * executionContext.getClaimedJobId() >> id
        1 * executionContext.getCurrentJobStatus() >> currentJobStatus
        1 * agentJobService.changeJobStatus(id, currentJobStatus, JobStatus.RUNNING, _ as String)
        1 * executionContext.setCurrentJobStatus(JobStatus.RUNNING)

        expect:
        event == Events.LAUNCH_JOB_COMPLETE
    }

    def "Successful with actual process"() {
        setup:
        process = new ProcessBuilder().command("echo").start()
        JobStatus currentJobStatus = JobStatus.INIT

        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobRunDirectory
        1 * executionContext.getJobEnvironment() >> jobEnvironment
        1 * jobSpec.getCommandArgs() >> jobCommandLine
        1 * jobSpec.isInteractive() >> interactive
        1 * launchJobService.launchProcess(jobRunDirectory, jobEnvironment, jobCommandLine, interactive)
        1 * executionContext.getClaimedJobId() >> id
        1 * executionContext.getCurrentJobStatus() >> currentJobStatus
        1 * agentJobService.changeJobStatus(id, currentJobStatus, JobStatus.RUNNING, _ as String)
        1 * executionContext.setCurrentJobStatus(JobStatus.RUNNING)

        expect:
        event == Events.LAUNCH_JOB_COMPLETE
    }

    def "Launch process exception"() {
        Exception exception = new JobLaunchException("")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobRunDirectory
        1 * executionContext.getJobEnvironment() >> jobEnvironment
        1 * jobSpec.getCommandArgs() >> jobCommandLine
        1 * jobSpec.isInteractive() >> interactive
        1 * launchJobService.launchProcess(jobRunDirectory, jobEnvironment, jobCommandLine, interactive) >> {throw exception}
        def e = thrown(RuntimeException)
        e.getCause() == exception
    }

    def "Change job status exception"() {
        Exception exception = new ChangeJobStatusException("")
        JobStatus currentJobStatus = JobStatus.INIT

        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobRunDirectory
        1 * executionContext.getJobEnvironment() >> jobEnvironment
        1 * jobSpec.getCommandArgs() >> jobCommandLine
        1 * jobSpec.isInteractive() >> interactive
        1 * launchJobService.launchProcess(jobRunDirectory, jobEnvironment, jobCommandLine, interactive)
        1 * executionContext.getClaimedJobId() >> id
        1 * executionContext.getCurrentJobStatus() >> currentJobStatus
        1 * agentJobService.changeJobStatus(id, currentJobStatus, JobStatus.RUNNING, _ as String) >> { throw exception }
        0 * executionContext.setCurrentJobStatus(_)
        def e = thrown(RuntimeException)
        e.getCause() == exception
    }
}
