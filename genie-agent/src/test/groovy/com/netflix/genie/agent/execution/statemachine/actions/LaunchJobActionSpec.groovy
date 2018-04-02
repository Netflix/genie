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
import com.netflix.genie.agent.execution.exceptions.JobLaunchException
import com.netflix.genie.agent.execution.services.LaunchJobService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.dto.v4.JobSpecification
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class LaunchJobActionSpec extends Specification {
    ExecutionContext executionContext
    JobSpecification jobSpec
    Map<String, String> jobEnvironment
    LaunchJobAction action
    LaunchJobService launchJobService
    Process process
    File jobRunDirectory
    List<String> jobCommandLine
    boolean interactive

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.jobSpec = Mock(JobSpecification)
        this.jobRunDirectory = Mock(File)
        this.jobEnvironment = Mock(Map)
        this.jobCommandLine = Mock(List)
        this.interactive = true
        this.launchJobService = Mock(LaunchJobService)
        this.process = Mock(Process)
        this.action = new LaunchJobAction(executionContext, launchJobService)
    }

    void cleanup() {
    }

    def "Success"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobRunDirectory
        1 * executionContext.getJobEnvironment() >> jobEnvironment
        1 * jobSpec.getCommandArgs() >> jobCommandLine
        1 * jobSpec.isInteractive() >> interactive
        1 * launchJobService.launchProcess(jobRunDirectory, jobEnvironment, jobCommandLine, interactive) >> process
        1 * executionContext.setJobProcess(process)

        expect:
        event == Events.LAUNCH_JOB_COMPLETE
    }

    def "Error"() {
        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobRunDirectory
        1 * executionContext.getJobEnvironment() >> jobEnvironment
        1 * jobSpec.getCommandArgs() >> jobCommandLine
        1 * jobSpec.isInteractive() >> interactive
        1 * launchJobService.launchProcess(jobRunDirectory, jobEnvironment, jobCommandLine, interactive) >> {throw new JobLaunchException("")}
        0 * executionContext.setJobProcess(process)
        def e = thrown(RuntimeException)
        e.getCause().getClass() == JobLaunchException
    }

    def "Actual process"() {
        setup:
        process = new ProcessBuilder().command("echo").start()

        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobRunDirectory
        1 * executionContext.getJobEnvironment() >> jobEnvironment
        1 * jobSpec.getCommandArgs() >> jobCommandLine
        1 * jobSpec.isInteractive() >> interactive
        1 * launchJobService.launchProcess(jobRunDirectory, jobEnvironment, jobCommandLine, interactive) >> process
        1 * executionContext.setJobProcess(process)

        expect:
        event == Events.LAUNCH_JOB_COMPLETE
    }

}
