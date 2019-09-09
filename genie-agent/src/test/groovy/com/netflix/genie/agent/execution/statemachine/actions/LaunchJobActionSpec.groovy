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
import com.netflix.genie.agent.execution.process.JobProcessManager
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import spock.lang.Specification

class LaunchJobActionSpec extends Specification {
    String id
    ExecutionContext executionContext
    JobSpecification jobSpec
    Map<String, String> jobEnvironment
    LaunchJobAction action
    JobProcessManager launchJobService
    AgentJobService agentJobService
    Process process
    File jobDirectory
    List<String> jobCommandLine
    boolean interactive

    void setup() {
        this.id = UUID.randomUUID().toString()
        this.executionContext = Mock(ExecutionContext)
        this.jobSpec = Mock(JobSpecification)
        this.jobDirectory = Mock(File)
        this.jobEnvironment = Mock(Map)
        this.jobCommandLine = Mock(List)
        this.interactive = true
        this.launchJobService = Mock(JobProcessManager)
        this.agentJobService = Mock(AgentJobService)
        this.process = Mock(Process)
        this.action = new LaunchJobAction(executionContext, launchJobService, agentJobService)
    }

    void cleanup() {
    }

    def "Successful"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> Optional.of(jobSpec)
        1 * executionContext.getJobDirectory() >> Optional.of(jobDirectory)
        1 * executionContext.getJobEnvironment() >> Optional.of(jobEnvironment)
        1 * jobSpec.getCommandArgs() >> jobCommandLine
        1 * jobSpec.isInteractive() >> interactive
        1 * launchJobService.launchProcess(jobDirectory, jobEnvironment, jobCommandLine, interactive)
        1 * executionContext.getClaimedJobId() >> Optional.of(id)
        1 * agentJobService.changeJobStatus(id, JobStatus.INIT, JobStatus.RUNNING, _ as String)
        1 * executionContext.setCurrentJobStatus(JobStatus.RUNNING)

        expect:
        event == Events.LAUNCH_JOB_COMPLETE
    }

    def "Successful with actual process"() {
        setup:
        process = new ProcessBuilder().command("echo").start()

        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> Optional.of(jobSpec)
        1 * executionContext.getJobDirectory() >> Optional.of(jobDirectory)
        1 * executionContext.getJobEnvironment() >> Optional.of(jobEnvironment)
        1 * jobSpec.getCommandArgs() >> jobCommandLine
        1 * jobSpec.isInteractive() >> interactive
        1 * launchJobService.launchProcess(jobDirectory, jobEnvironment, jobCommandLine, interactive)
        1 * executionContext.getClaimedJobId() >> Optional.of(id)
        1 * agentJobService.changeJobStatus(id, JobStatus.INIT, JobStatus.RUNNING, _ as String)
        1 * executionContext.setCurrentJobStatus(JobStatus.RUNNING)

        expect:
        event == Events.LAUNCH_JOB_COMPLETE
    }

    def "Launch process exception"() {
        Exception exception = new JobLaunchException("")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> Optional.of(jobSpec)
        1 * executionContext.getJobDirectory() >> Optional.of(jobDirectory)
        1 * executionContext.getJobEnvironment() >> Optional.of(jobEnvironment)
        1 * jobSpec.getCommandArgs() >> jobCommandLine
        1 * jobSpec.isInteractive() >> interactive
        1 * launchJobService.launchProcess(jobDirectory, jobEnvironment, jobCommandLine, interactive) >> {
            throw exception
        }
        def e = thrown(RuntimeException)
        e.getCause() == exception
    }

    def "Change job status exception"() {
        Exception exception = new ChangeJobStatusException("")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> Optional.of(jobSpec)
        1 * executionContext.getJobDirectory() >> Optional.of(jobDirectory)
        1 * executionContext.getJobEnvironment() >> Optional.of(jobEnvironment)
        1 * jobSpec.getCommandArgs() >> jobCommandLine
        1 * jobSpec.isInteractive() >> interactive
        1 * launchJobService.launchProcess(jobDirectory, jobEnvironment, jobCommandLine, interactive)
        1 * executionContext.getClaimedJobId() >> Optional.of(id)
        1 * agentJobService.changeJobStatus(id, JobStatus.INIT, JobStatus.RUNNING, _ as String) >> { throw exception }
        0 * executionContext.setCurrentJobStatus(_)
        def e = thrown(RuntimeException)
        e.getCause() == exception
    }

    def "Pre and post action validation"() {
        when:
        action.executePreActionValidation()

        then:
        1 * executionContext.getClaimedJobId() >> Optional.of(id)
        1 * executionContext.getCurrentJobStatus() >> Optional.of(JobStatus.INIT)
        1 * executionContext.getJobSpecification() >> Optional.of(jobSpec)
        1 * executionContext.getJobDirectory() >> Optional.of(jobDirectory)
        1 * executionContext.getJobEnvironment() >> Optional.of(jobEnvironment)

        when:
        action.executePostActionValidation()

        then:
        1 * executionContext.getCurrentJobStatus() >> Optional.of(JobStatus.RUNNING)
    }
}
