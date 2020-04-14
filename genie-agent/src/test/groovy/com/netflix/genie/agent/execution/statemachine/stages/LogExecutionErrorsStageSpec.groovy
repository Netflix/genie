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
package com.netflix.genie.agent.execution.statemachine.stages

import com.netflix.genie.agent.execution.exceptions.HandshakeException
import com.netflix.genie.agent.execution.exceptions.SetUpJobException
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.States
import org.assertj.core.util.Lists
import spock.lang.Specification

class LogExecutionErrorsStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    List<ExecutionContext.TransitionExceptionRecord> records

    void setup() {
        this.records = Lists.newArrayList()
        this.executionContext = Mock(ExecutionContext)
        this.stage = new LogExecutionErrorsStage()
    }

    def "AttemptTransition"() {
        setup:
        records.add(
            new ExecutionContext.TransitionExceptionRecord(
                States.CREATE_JOB_DIRECTORY, new SetUpJobException("...")
            )
        )
        records.add(
            new ExecutionContext.TransitionExceptionRecord(
                States.HANDSHAKE, new HandshakeException("...", new IOException())
            )
        )

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getTransitionExceptionRecords() >> records
    }

    def "AttemptTransition -- empty"() {
        setup:

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getTransitionExceptionRecords() >> records
    }
}
