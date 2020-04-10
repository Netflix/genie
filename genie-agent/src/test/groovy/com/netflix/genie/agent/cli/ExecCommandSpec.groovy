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
import com.netflix.genie.agent.execution.statemachine.FatalTransitionException
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachine
import com.netflix.genie.agent.execution.statemachine.States
import com.netflix.genie.common.external.dtos.v4.JobStatus
import spock.lang.Specification
import spock.lang.Unroll

class ExecCommandSpec extends Specification {
    ExecCommand.ExecCommandArguments args
    JobExecutionStateMachine stateMachine;
    ExecutionContext execContext
    KillService killService

    void setup() {
        this.args = Mock(ExecCommand.ExecCommandArguments)
        this.stateMachine = Mock(JobExecutionStateMachine)
        this.execContext = Mock(ExecutionContext)
        this.killService = Mock(KillService)
    }

    def "Run"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * stateMachine.run()
        1 * stateMachine.getExecutionContext() >> execContext
        1 * execContext.getCurrentJobStatus() >> JobStatus.SUCCEEDED
        exitCode == ExitCode.SUCCESS
    }

    def "Run with no final job status"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService)

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
        def execCommand = new ExecCommand(args, stateMachine, killService)

        when:
        execCommand.run()

        then:
        1 * stateMachine.run() >> { throw new RuntimeException() }

        thrown(RuntimeException.class)
    }


    @Unroll
    def "Exec fail (launched: #launched)"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * stateMachine.run()
        1 * stateMachine.getExecutionContext() >> execContext
        1 * execContext.getCurrentJobStatus() >> JobStatus.FAILED
        1 * execContext.isJobLaunched() >> launched
        1 * execContext.getExecutionAbortedFatalException() >> new FatalTransitionException(States.CREATE_JOB_DIRECTORY, "...", new IOException())

        exitCode == expectedExitCode

        where:
        launched | expectedExitCode
        true     | ExitCode.EXEC_FAIL
        false    | ExitCode.COMMAND_INIT_FAIL

    }

    def "Kill"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * stateMachine.run()
        1 * stateMachine.getExecutionContext() >> execContext
        1 * execContext.getCurrentJobStatus() >> JobStatus.KILLED
        exitCode == ExitCode.EXEC_ABORTED
    }

    def "Early setup failure"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * stateMachine.run()
        1 * stateMachine.getExecutionContext() >> execContext
        1 * execContext.getCurrentJobStatus() >> JobStatus.INVALID
        exitCode == ExitCode.INIT_FAIL
    }

    def "Run with invalid final job status"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService)

        when:
        execCommand.run()

        then:
        1 * stateMachine.run()
        1 * stateMachine.getExecutionContext() >> execContext
        1 * execContext.getCurrentJobStatus() >> JobStatus.RUNNING

        thrown(RuntimeException)
    }

    def "Handle ctrl-c"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, killService)

        when:
        execCommand.handleTerminationSignal()

        then:
        1 * killService.kill(KillService.KillSource.SYSTEM_SIGNAL)
    }
}
