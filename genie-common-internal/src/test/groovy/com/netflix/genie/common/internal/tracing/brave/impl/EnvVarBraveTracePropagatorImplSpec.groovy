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
package com.netflix.genie.common.internal.tracing.brave.impl

import brave.propagation.TraceContext
import spock.lang.Specification

/**
 * Specifications for {@link EnvVarBraveTracePropagatorImpl}.
 *
 * @author tgianos
 */
class EnvVarBraveTracePropagatorImplSpec extends Specification {

    EnvVarBraveTracePropagatorImpl propagator

    def setup() {
        this.propagator = new EnvVarBraveTracePropagatorImpl()
    }

    def "Extraction edge cases produce empty context"() {
        def mockMap = Mock(Map)
        def environment = new HashMap<String, String>()

        when:
        def context = this.propagator.extract(mockMap)

        then:
        1 * mockMap.get(_ as String) >> {
            throw new RuntimeException("whoops")
        }
        noExceptionThrown()
        !context.isPresent()

        when:
        context = this.propagator.extract(environment)

        then:
        noExceptionThrown()
        !context.isPresent()

        when: "Trace id is present but no span id"
        environment.put(
            EnvVarBraveTracePropagatorImpl.GENIE_AGENT_B3_TRACE_ID_LOW_KEY,
            Long.toString(UUID.randomUUID().getMostSignificantBits())
        )
        context = this.propagator.extract(environment)

        then: "Empty context is returned"
        noExceptionThrown()
        !context.isPresent()
    }

    def "Can inject and extract for agent propagation"() {
        def traceIdLow = UUID.randomUUID().getMostSignificantBits()
        def traceIdHigh = UUID.randomUUID().getLeastSignificantBits()
        def spanId = UUID.randomUUID().getLeastSignificantBits()
        def sampled = true
        def parentSpanId = UUID.randomUUID().getMostSignificantBits()

        def parentTraceContext = TraceContext.newBuilder()
            .traceId(traceIdLow)
            .traceIdHigh(traceIdHigh)
            .sampled(sampled)
            .parentId(parentSpanId)
            .spanId(spanId)
            .build()

        when:
        def environment = this.propagator.injectForAgent(parentTraceContext)

        then:
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_AGENT_B3_TRACE_ID_LOW_KEY) == Long.toString(traceIdLow)
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_AGENT_B3_TRACE_ID_HIGH_KEY) == Long.toString(traceIdHigh)
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_AGENT_B3_SPAN_ID_KEY) == Long.toString(spanId)
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_AGENT_B3_PARENT_SPAN_ID_KEY) == Long.toString(parentSpanId)
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_AGENT_B3_SAMPLED_KEY) == Boolean.toString(sampled)
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_JOB_B3_TRACE_ID_LOW_KEY) == null
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_JOB_B3_TRACE_ID_HIGH_KEY) == null
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_JOB_B3_PARENT_SPAN_ID_KEY) == null
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_JOB_B3_SAMPLED_KEY) == null

        when:
        def childTraceContext = this.propagator.extract(environment).get()

        then:
        childTraceContext != null
        childTraceContext.traceIdString() == parentTraceContext.traceIdString()
        childTraceContext.spanIdString() == parentTraceContext.spanIdString()
        childTraceContext.parentIdString() == parentTraceContext.parentIdString()
        childTraceContext.sampled() == parentTraceContext.sampled()
    }

    def "Can inject for job propagation"() {
        def traceIdLow = UUID.randomUUID().getMostSignificantBits()
        def sampled = false
        def spanId = UUID.randomUUID().getLeastSignificantBits()

        def parentTraceContext = TraceContext.newBuilder()
            .traceId(traceIdLow)
            .sampled(sampled)
            .spanId(spanId)
            .build()

        when:
        def environment = this.propagator.injectForJob(parentTraceContext)

        then:
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_AGENT_B3_TRACE_ID_LOW_KEY) == null
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_AGENT_B3_TRACE_ID_HIGH_KEY) == null
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_AGENT_B3_PARENT_SPAN_ID_KEY) == null
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_AGENT_B3_SAMPLED_KEY) == null
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_JOB_B3_TRACE_ID_LOW_KEY) == Long.toString(traceIdLow)
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_JOB_B3_TRACE_ID_HIGH_KEY) == null
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_JOB_B3_SPAN_ID_KEY) == Long.toString(spanId)
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_JOB_B3_PARENT_SPAN_ID_KEY) == null
        environment.get(EnvVarBraveTracePropagatorImpl.GENIE_JOB_B3_SAMPLED_KEY) == Boolean.toString(sampled)
    }
}
