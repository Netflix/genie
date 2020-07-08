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
package com.netflix.genie.agent.cli

import com.netflix.genie.agent.execution.services.KillService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachine
import com.netflix.genie.agent.execution.statemachine.States
import com.netflix.genie.agent.properties.AgentProperties
import com.netflix.genie.common.external.dtos.v4.JobStatus
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration
import java.util.concurrent.ThreadFactory

class ExecCommandSpec extends Specification {
    ExecCommand.ExecCommandArguments args
    JobExecutionStateMachine stateMachine;
    ExecutionContext execContext
    KillService killService
    ThreadFactory threadFactory
    AgentProperties agentProperties

    void setup() {
        this.args = Mock(ExecCommand.ExecCommandArguments)
        this.stateMachine = Mock(JobExecutionStateMachine)
        this.execContext = Mock(ExecutionContext)
        this.killService = Mock(KillService)
        this.agentProperties = new AgentProperties()
        this.threadFactory = { r -> Mock(Thread) }

        this.agentProperties.getShutdown().setExecutionCompletionLeeway(Duration.ofSeconds(1))
    }

    def "Run"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService, agentProperties, threadFactory)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * stateMachine.run()
        1 * stateMachine.getExecutionContext() >> execContext
        1 * execContext.getCurrentJobStatus() >> JobStatus.SUCCEEDED
        exitCode == ExitCode.SUCCESS

        when:
        execCommand.handleSystemSignal()
        execCommand.waitForCleanShutdown()

        then:
        noExceptionThrown()
    }

    def "Run with no final job status"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService, agentProperties, threadFactory)

        when:
        execCommand.run()

        then:
        1 * stateMachine.run()
        1 * stateMachine.getExecutionContext() >> execContext
        1 * execContext.getCurrentJobStatus() >> null

        thrown(RuntimeException.class)
    }

    def "Run fail"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService, agentProperties, threadFactory)

        when:
        execCommand.run()

        then:
        1 * stateMachine.run() >> { throw new RuntimeException() }

        thrown(RuntimeException.class)
    }


    @Unroll
    def "Exec fail (launched: #launched)"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService, agentProperties, threadFactory)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * stateMachine.run()
        1 * stateMachine.getExecutionContext() >> execContext
        1 * execContext.getCurrentJobStatus() >> JobStatus.FAILED
        1 * execContext.isJobLaunched() >> launched
        1 * execContext.getExecutionAbortedFatalException() >> new FatalJobExecutionException(States.CREATE_JOB_DIRECTORY, "...", new IOException())

        exitCode == expectedExitCode

        when:
        execCommand.handleSystemSignal()
        execCommand.waitForCleanShutdown()

        then:
        noExceptionThrown()

        where:
        launched | expectedExitCode
        true     | ExitCode.EXEC_FAIL
        false    | ExitCode.COMMAND_INIT_FAIL

    }

    def "Kill"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService, agentProperties, threadFactory)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * stateMachine.run()
        1 * stateMachine.getExecutionContext() >> execContext
        1 * execContext.getCurrentJobStatus() >> JobStatus.KILLED
        exitCode == ExitCode.EXEC_ABORTED

        when:
        execCommand.handleSystemSignal()
        execCommand.waitForCleanShutdown()

        then:
        noExceptionThrown()
    }

    def "Early setup failure"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService, agentProperties, threadFactory)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * stateMachine.run()
        1 * stateMachine.getExecutionContext() >> execContext
        1 * execContext.getCurrentJobStatus() >> JobStatus.INVALID
        exitCode == ExitCode.INIT_FAIL

        when:
        execCommand.handleSystemSignal()
        execCommand.waitForCleanShutdown()

        then:
        noExceptionThrown()
    }

    def "Run with invalid final job status"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService, agentProperties, threadFactory)

        when:
        execCommand.run()

        then:
        1 * stateMachine.run()
        1 * stateMachine.getExecutionContext() >> execContext
        1 * execContext.getCurrentJobStatus() >> JobStatus.RUNNING

        thrown(RuntimeException)

        when:
        execCommand.handleSystemSignal()
        execCommand.waitForCleanShutdown()

        then:
        noExceptionThrown()
    }
}
