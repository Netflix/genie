/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.util

import com.google.common.collect.Sets
import com.netflix.genie.common.dto.JobRequest
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.springframework.core.env.Environment
import spock.lang.Specification

import javax.servlet.http.HttpServletRequest


class JobExecutionModeSelectorSpec extends Specification {
    Random random
    Environment environment
    MeterRegistry meterRegistry
    Counter counter
    JobRequest jobRequest
    HttpServletRequest httpServletRequest


    void setup() {
        this.random = Mock(Random)
        this.environment = Mock(Environment)
        this.counter = Mock(Counter)
        this.meterRegistry = Mock(MeterRegistry) {
            counter(_ as String, _ as Iterable<Tag>) >> this.counter
        }
        this.jobRequest = Mock(JobRequest)
        this.httpServletRequest = Mock(HttpServletRequest)
    }

    def "ExecuteWithAgent with no configuration (using default constructor)"() {
        setup:
        JobExecutionModeSelector selector = new JobExecutionModeSelector(environment, meterRegistry)

        when:
        boolean executeWithAgent = selector.executeWithAgent(jobRequest, httpServletRequest)

        then:
        executeWithAgent == JobExecutionModeSelector.DEFAULT_EXECUTE_WITH_AGENT
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_AGENT_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_EMBEDDED_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_AGENT_EXECUTION_HEADER_NAME)
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_EMBEDDED_EXECUTION_HEADER_NAME)
        1 * this.environment.getProperty(JobExecutionModeSelector.AGENT_PROBABILITY_PROPERTY)
        1 * this.environment.getProperty(JobExecutionModeSelector.DEFAULT_EXECUTE_WITH_AGENT_PROPERTY, Boolean, false) >> false
        1 * this.meterRegistry.counter(JobExecutionModeSelector.METRIC_NAME, getTags("default", false)) >> this.counter
        1 * this.counter.increment()
    }

    def "ExecuteWithAgent"() {
        setup:
        JobExecutionModeSelector selector = new JobExecutionModeSelector(random, environment, meterRegistry)
        boolean executeWithAgent

        when: "Global agent override is set"
        executeWithAgent = selector.executeWithAgent(jobRequest, httpServletRequest)

        then:
        executeWithAgent
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_AGENT_OVERRIDE_PROPERTY, Boolean, false) >> true
        1 * this.meterRegistry.counter(JobExecutionModeSelector.METRIC_NAME, getTags("global-override", true)) >> this.counter
        1 * this.counter.increment()

        when: "Global embedded override is set"
        executeWithAgent = selector.executeWithAgent(jobRequest, httpServletRequest)

        then:
        !executeWithAgent
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_AGENT_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_EMBEDDED_OVERRIDE_PROPERTY, Boolean, false) >> true
        1 * this.meterRegistry.counter(JobExecutionModeSelector.METRIC_NAME, getTags("global-override", false)) >> this.counter
        1 * this.counter.increment()

        when: "Agent header override is set"
        executeWithAgent = selector.executeWithAgent(jobRequest, httpServletRequest)

        then:
        executeWithAgent
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_AGENT_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_EMBEDDED_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_AGENT_EXECUTION_HEADER_NAME) >> "true"
        1 * this.meterRegistry.counter(JobExecutionModeSelector.METRIC_NAME, getTags("header-override", true)) >> this.counter
        1 * this.counter.increment()

        when: "Embedded header override is set"
        executeWithAgent = selector.executeWithAgent(jobRequest, httpServletRequest)

        then:
        !executeWithAgent
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_AGENT_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_EMBEDDED_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_AGENT_EXECUTION_HEADER_NAME)
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_EMBEDDED_EXECUTION_HEADER_NAME) >> "true"
        1 * this.meterRegistry.counter(JobExecutionModeSelector.METRIC_NAME, getTags("header-override", false)) >> this.counter
        1 * this.counter.increment()

        when: "Probability threshold is set -- selected for agent execution"
        executeWithAgent = selector.executeWithAgent(jobRequest, httpServletRequest)

        then:
        executeWithAgent
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_AGENT_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_EMBEDDED_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_AGENT_EXECUTION_HEADER_NAME)
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_EMBEDDED_EXECUTION_HEADER_NAME)
        1 * this.environment.getProperty(JobExecutionModeSelector.AGENT_PROBABILITY_PROPERTY) >> "0.8"
        1 * this.random.nextFloat() >> 0.5f
        1 * this.meterRegistry.counter(JobExecutionModeSelector.METRIC_NAME, getTags("percentage", true)) >> this.counter
        1 * this.counter.increment()

        when: "Probability threshold is set -- selected for embedded execution"
        executeWithAgent = selector.executeWithAgent(jobRequest, httpServletRequest)

        then:
        !executeWithAgent
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_AGENT_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_EMBEDDED_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_AGENT_EXECUTION_HEADER_NAME)
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_EMBEDDED_EXECUTION_HEADER_NAME)
        1 * this.environment.getProperty(JobExecutionModeSelector.AGENT_PROBABILITY_PROPERTY) >> "0.8"
        1 * this.random.nextFloat() >> 0.8f
        1 * this.meterRegistry.counter(JobExecutionModeSelector.METRIC_NAME, getTags("percentage", false)) >> this.counter
        1 * this.counter.increment()

        when: "Fall-through to default (with probability threshold is set to invalid value)"
        executeWithAgent = selector.executeWithAgent(jobRequest, httpServletRequest)

        then:
        executeWithAgent
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_AGENT_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_EMBEDDED_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_AGENT_EXECUTION_HEADER_NAME)
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_EMBEDDED_EXECUTION_HEADER_NAME)
        1 * this.environment.getProperty(JobExecutionModeSelector.AGENT_PROBABILITY_PROPERTY) >> "30"
        1 * this.environment.getProperty(JobExecutionModeSelector.DEFAULT_EXECUTE_WITH_AGENT_PROPERTY, Boolean, false) >> true
        1 * this.meterRegistry.counter(JobExecutionModeSelector.METRIC_NAME, getTags("default", true)) >> this.counter
        1 * this.counter.increment()

        when: "Fall-through to default"
        executeWithAgent = selector.executeWithAgent(jobRequest, httpServletRequest)

        then:
        !executeWithAgent
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_AGENT_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * this.environment.getProperty(JobExecutionModeSelector.GLOBAL_EMBEDDED_OVERRIDE_PROPERTY, Boolean, false) >> false
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_AGENT_EXECUTION_HEADER_NAME)
        1 * httpServletRequest.getHeader(JobExecutionModeSelector.FORCE_EMBEDDED_EXECUTION_HEADER_NAME)
        1 * this.environment.getProperty(JobExecutionModeSelector.AGENT_PROBABILITY_PROPERTY)
        1 * this.environment.getProperty(JobExecutionModeSelector.DEFAULT_EXECUTE_WITH_AGENT_PROPERTY, Boolean, false) >> false
        1 * this.meterRegistry.counter(JobExecutionModeSelector.METRIC_NAME, getTags("default", false)) >> this.counter
        1 * this.counter.increment()
    }

    def getTags(String checkName, boolean expectedDecision) {
        return Sets.newHashSet(
            Tag.of(JobExecutionModeSelector.OUTCOME_METRIC_TAG_NAME, String.valueOf(expectedDecision)),
            Tag.of(JobExecutionModeSelector.CHECK_METRIC_TAG_NAME, checkName)
        )
    }
}
