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

import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.agent.execution.statemachine.States
import spock.lang.Specification

import javax.validation.constraints.NotBlank

class StartServiceStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    TestService service
    def jobId

    void setup() {
        this.jobId = UUID.randomUUID().toString()
        this.executionContext = Mock(ExecutionContext)
        this.service = Mock(TestService)
        this.stage = new StartTestServiceStage(service)
    }

    def "AttemptTransition -- success"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getClaimedJobId() >> jobId
        1 * service.start(jobId)
    }

    def "AttemptTransition -- error"() {
        setup:
        Exception argumentException = Mock(IllegalArgumentException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getClaimedJobId() >> jobId
        1 * service.start(jobId) >> { throw argumentException }
        def e = thrown(FatalJobExecutionException)
        e.getCause() == argumentException
    }

    interface TestService {
        void start(String jobId)
    }

    class StartTestServiceStage extends StartServiceStage {

        private TestService testService

        StartTestServiceStage(TestService testService) {
            super(States.START_KILL_SERVICE)
            this.testService = testService
        }

        @Override
        protected void startService(final @NotBlank String claimedJobId, final ExecutionContext executionContext) {
            this.testService.start(claimedJobId)
        }
    }
}
