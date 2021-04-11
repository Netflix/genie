/*
 *
 *  Copyright 2021 Netflix, Inc.
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
package com.netflix.genie.agent.execution.statemachine.listeners

import brave.SpanCustomizer
import brave.Tracer
import com.netflix.genie.agent.execution.statemachine.States
import spock.lang.Specification

/**
 * Specifications for {@link TracingListener}.
 *
 * @author tgianos
 */
class TracingListenerSpec extends Specification {

    SpanCustomizer spanCustomizer
    Tracer tracer
    TracingListener listener

    def setup() {
        this.tracer = Mock(Tracer)
        this.spanCustomizer = Mock(SpanCustomizer)
        this.listener = new TracingListener(this.tracer)
    }

    def "State entered behavior is correct"() {
        when:
        this.listener.stateEntered(States.CLAIM_JOB)

        then:
        1 * this.tracer.currentSpanCustomizer() >> this.spanCustomizer
        1 * this.spanCustomizer.annotate(TracingListener.ENTERED_STATE_ANNOTATION_PREFIX + States.CLAIM_JOB)
    }

    def "State exited behavior is correct"() {
        when:
        this.listener.stateExited(States.CREATE_JOB_SCRIPT)

        then:
        1 * this.tracer.currentSpanCustomizer() >> this.spanCustomizer
        1 * this.spanCustomizer.annotate(TracingListener.EXITED_STATE_ANNOTATION_PREFIX + States.CREATE_JOB_SCRIPT)
    }
}
