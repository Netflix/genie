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

import com.google.common.collect.Lists
import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.services.KillService
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachine
import com.netflix.genie.agent.execution.statemachine.States
import com.netflix.genie.agent.execution.statemachine.actions.BaseStateAction
import com.netflix.genie.common.external.dtos.v4.JobStatus
import org.apache.commons.lang3.tuple.Triple
import org.springframework.statemachine.action.Action
import spock.lang.Specification

class ExecCommandSpec extends Specification {
    ExecCommand.ExecCommandArguments args
    JobExecutionStateMachine stateMachine;
    ExecutionContext execContext
    KillService killService

    void setup() {
        args = Mock()
        stateMachine = Mock()
        execContext = Mock()
        killService = Mock()
    }

    void cleanup() {
    }

    def "Run"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, execContext, killService)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * stateMachine.start()
        1 * stateMachine.waitForStop() >> States.END
        1 * execContext.getFinalJobStatus() >> Optional.of(JobStatus.SUCCEEDED)
        exitCode == ExitCode.SUCCESS
    }

    def "Run with no final job status"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, execContext, killService)

        when:
        execCommand.run()

        then:
        1 * stateMachine.start()
        1 * stateMachine.waitForStop() >> States.END
        1 * execContext.getFinalJobStatus() >> Optional.empty()

        thrown(RuntimeException.class)
    }

    def "Run fail"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, execContext, killService)

        when:
        execCommand.run()

        then:
        1 * stateMachine.start()
        1 * stateMachine.waitForStop() >> null

        thrown(RuntimeException.class)
    }

    def "Exec error"() {
        setup:
        List<Triple<States, Class<? extends Action>, Exception>> actionErrors = Lists.newArrayList(
            Triple.of(States.SETUP_JOB, BaseStateAction.class, new RuntimeException())
        )
        def execCommand = new ExecCommand(args, stateMachine, execContext, killService)

        when:
        execCommand.run()

        then:
        1 * stateMachine.start()
        1 * stateMachine.waitForStop() >> States.END
        1 * execContext.hasStateActionError() >> true
        1 * execContext.getStateActionErrors() >> actionErrors

        thrown(RuntimeException.class)
    }

    def "Exec fail"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, execContext, killService)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * stateMachine.start()
        1 * stateMachine.waitForStop() >> States.END
        1 * execContext.getFinalJobStatus() >> Optional.of(JobStatus.FAILED)
        exitCode == ExitCode.EXEC_FAIL
    }

    def "Kill"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, execContext, killService)

        when:
        ExitCode exitCode = execCommand.run()

        then:
        1 * stateMachine.start()
        1 * stateMachine.waitForStop() >> States.END
        1 * execContext.getFinalJobStatus() >> Optional.of(JobStatus.KILLED)
        exitCode == ExitCode.EXEC_ABORTED
    }

    def "Run with invalid final job status"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, execContext, killService)

        when:
        execCommand.run()

        then:
        1 * stateMachine.start()
        1 * stateMachine.waitForStop() >> States.END
        1 * execContext.getFinalJobStatus() >> Optional.of(JobStatus.INVALID)

        thrown(RuntimeException)
    }

    def "Run with non-terminal final job status"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, execContext, killService)

        when:
        execCommand.run()

        then:
        1 * stateMachine.start()
        1 * stateMachine.waitForStop() >> States.END
        1 * execContext.getFinalJobStatus() >> Optional.of(JobStatus.CLAIMED)

        thrown(RuntimeException.class)
    }

    def "Run interrupted"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, execContext, killService)

        when:
        execCommand.run()

        then:
        1 * stateMachine.start()
        1 * stateMachine.waitForStop() >> { throw new InterruptedException() }

        thrown(RuntimeException.class)
    }

    def "Handle ctrl-c"() {
        setup:
        def execCommand = new ExecCommand(args, stateMachine, execContext, killService)

        when:
        execCommand.handleTerminationSignal()

        then:
        1 * execContext.getCurrentJobStatus() >> Optional.empty()
        1 * killService.kill(KillService.KillSource.SYSTEM_SIGNAL)
    }
}
