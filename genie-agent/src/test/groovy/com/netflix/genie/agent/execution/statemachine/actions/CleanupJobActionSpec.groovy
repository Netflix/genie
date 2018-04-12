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
import com.netflix.genie.agent.execution.statemachine.Events
import org.assertj.core.util.Lists
import spock.lang.Specification

class CleanupJobActionSpec extends Specification {
    ExecutionContext executionContext
    CleanupJobAction action
    List<StateAction> cleanupQueue


    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.action = new CleanupJobAction(executionContext)
        this.cleanupQueue = Lists.newArrayList()

        executionContext.getCleanupActions() >> cleanupQueue
    }

    void cleanup() {
    }

    def "Execute with empty queue"() {
        when:
        def event = action.executeStateAction(executionContext)
        then:
        event == Events.CLEANUP_JOB_COMPLETE
    }


    def "Execute with actions and self"() {
        setup:
        def action1 = Mock(StateAction)
        def action2 = Mock(StateAction)
        cleanupQueue.add(action1)
        cleanupQueue.add(action2)
        cleanupQueue.add(action)

        when:
        def event = action.executeStateAction(executionContext)

        then:
        event == Events.CLEANUP_JOB_COMPLETE
        1 * action1.cleanup()
        1 * action2.cleanup()
        0 * action.cleanup()
    }

    def "Execute with throwing action"() {
        setup:
        def action1 = Mock(StateAction)
        def action2 = Mock(StateAction)
        cleanupQueue.add(action1)
        cleanupQueue.add(action2)
        cleanupQueue.add(action)

        when:
        def event = action.executeStateAction(executionContext)

        then:
        event == Events.CLEANUP_JOB_COMPLETE
        1 * action1.cleanup()
        1 * action2.cleanup() >> {throw new RuntimeException()}
        0 * action.cleanup()
    }
}
