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

class StopServiceStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    TestService service

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.service = Mock(TestService)
        this.stage = new StopTestServiceStage(service)
    }

    def "AttemptTransition -- success"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * service.stop()
    }

    def "AttemptTransition -- error"() {
        setup:
        Exception runtimeException = Mock(RuntimeException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * service.stop() >> { throw runtimeException }
        def e = thrown(FatalJobExecutionException)
        e.getCause() == runtimeException
    }

    interface TestService {
        void stop()
    }

    class StopTestServiceStage extends StopServiceStage {

        private TestService testService

        StopTestServiceStage(TestService testService) {
            super(States.START_KILL_SERVICE)
            this.testService = testService
        }

        @Override
        protected void stopService() {
            this.testService.stop()
        }
    }
}
