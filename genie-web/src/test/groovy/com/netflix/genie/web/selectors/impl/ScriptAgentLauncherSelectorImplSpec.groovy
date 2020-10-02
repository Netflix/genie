/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.selectors.impl

import com.google.common.collect.Sets
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata
import com.netflix.genie.web.agent.launchers.AgentLauncher
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.dtos.ResourceSelectionResult
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException
import com.netflix.genie.web.scripts.AgentLauncherSelectorManagedScript
import com.netflix.genie.web.scripts.ResourceSelectorScriptResult
import com.netflix.genie.web.selectors.AgentLauncherSelectionContext
import com.netflix.genie.web.util.MetricsConstants
import com.netflix.genie.web.util.MetricsUtils
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * Specifications for the {@link ScriptAgentLauncherSelectorImpl} class.
 */
@SuppressWarnings("GroovyAccessibility")
class ScriptAgentLauncherSelectorImplSpec extends Specification {

    AgentLauncherSelectorManagedScript script
    MeterRegistry registry
    ScriptAgentLauncherSelectorImpl scriptAgentLauncherSelector
    Timer timer
    Collection<AgentLauncher> agentLaunchers
    AgentLauncher agentLauncher1 = Mock(AgentLauncher)
    AgentLauncher agentLauncher2 = Mock(AgentLauncher)
    JobRequestMetadata jobRequestMetadata
    ResolvedJob resolvedJob

    def setup() {
        this.timer = Mock(Timer)
        this.script = Mock(AgentLauncherSelectorManagedScript)
        this.registry = Mock(MeterRegistry)
        this.jobRequestMetadata = Mock(JobRequestMetadata)
        this.resolvedJob = Mock(ResolvedJob)
        this.agentLaunchers = Sets.newHashSet(agentLauncher1, agentLauncher2)
        this.scriptAgentLauncherSelector = new ScriptAgentLauncherSelectorImpl(this.script, this.agentLaunchers, this.registry)
    }

    def "can select a agentLauncher"() {
        JobRequest jobRequest = Mock(JobRequest)
        String jobId = UUID.randomUUID().toString()
        Throwable executionException = new ScriptExecutionException("some error")
        AgentLauncherSelectionContext context = new AgentLauncherSelectionContext(
            jobId,
            jobRequest,
            jobRequestMetadata,
            resolvedJob,
            scriptAgentLauncherSelector.getAgentLaunchers()
        )

        ResourceSelectionResult<AgentLauncher> result
        Set<Tag> expectedTags
        ResourceSelectorScriptResult<AgentLauncher> scriptResult = Mock(ResourceSelectorScriptResult)

        when: "Script returns null"
        expectedTags = MetricsUtils.newSuccessTagsSet()
        expectedTags.add(Tag.of(MetricsConstants.TagKeys.AGENT_LAUNCHER_CLASS, ScriptAgentLauncherSelectorImpl.NULL_TAG))
        result = this.scriptAgentLauncherSelector.select(context)

        then:
        1 * this.script.selectResource(context) >> scriptResult
        1 * scriptResult.getResource() >> Optional.empty()
        1 * scriptResult.getRationale() >> Optional.empty()
        1 * this.registry.timer(
            ScriptAgentLauncherSelectorImpl.SELECT_TIMER_NAME,
            {
                it == expectedTags
            }
        ) >> this.timer
        1 * this.timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        !result.getSelectedResource().isPresent()
        result.getSelectionRationale().orElse(null) == ScriptAgentLauncherSelectorImpl.NULL_RATIONALE
        result.getSelectorClass() == ScriptAgentLauncherSelectorImpl.class

        when: "Script throws"
        expectedTags = MetricsUtils.newFailureTagsSetForException(executionException)
        this.scriptAgentLauncherSelector.select(context)

        then:
        1 * this.script.selectResource(context) >> { throw executionException }
        1 * this.registry.timer(
            ScriptAgentLauncherSelectorImpl.SELECT_TIMER_NAME,
            { it == expectedTags }
        ) >> this.timer
        1 * this.timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        thrown(ResourceSelectionException)

        when: "Script selects agentLauncher"
        expectedTags = MetricsUtils.newSuccessTagsSet()
        expectedTags.add(Tag.of(MetricsConstants.TagKeys.AGENT_LAUNCHER_CLASS, agentLauncher2.getClass().getSimpleName()))
        result = this.scriptAgentLauncherSelector.select(context)

        then:
        1 * this.script.selectResource(context) >> scriptResult
        1 * scriptResult.getResource() >> Optional.of(agentLauncher2)
        1 * scriptResult.getRationale() >> Optional.of("AgentLauncher 2 was good")
        1 * this.registry.timer(
            ScriptAgentLauncherSelectorImpl.SELECT_TIMER_NAME,
            { it == expectedTags }
        ) >> this.timer
        1 * this.timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        result.getSelectedResource().orElse(null) == agentLauncher2
        result.getSelectionRationale().orElse(null) == "AgentLauncher 2 was good"
        result.getSelectorClass() == ScriptAgentLauncherSelectorImpl.class
    }
}
