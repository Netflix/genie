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

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Sets
import com.netflix.genie.common.external.dtos.v4.Cluster
import com.netflix.genie.common.external.dtos.v4.Command
import com.netflix.genie.common.external.dtos.v4.CommandMetadata
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.web.dtos.ResourceSelectionResult
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException
import com.netflix.genie.web.scripts.CommandSelectorManagedScript
import com.netflix.genie.web.scripts.ResourceSelectorScriptResult
import com.netflix.genie.web.selectors.CommandSelectionContext
import com.netflix.genie.web.util.MetricsConstants
import com.netflix.genie.web.util.MetricsUtils
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * Specifications for the {@link ScriptCommandSelectorImpl} class.
 */
class ScriptCommandSelectorImplSpec extends Specification {

    CommandSelectorManagedScript script
    MeterRegistry registry
    ScriptCommandSelectorImpl commandSelector
    Timer timer

    def setup() {
        this.timer = Mock(Timer)
        this.script = Mock(CommandSelectorManagedScript)
        this.registry = Mock(MeterRegistry)
        this.commandSelector = new ScriptCommandSelectorImpl(this.script, this.registry)
    }

    def "Can select command"() {
        def command0 = Mock(Command)
        def command1 = Mock(Command)
        def command1Metadata = Mock(CommandMetadata)
        def clusters = Sets.newHashSet(Mock(Cluster), Mock(Cluster))
        def commandClusters = ImmutableMap.of(
            command0, clusters,
            command1, clusters
        )
        def jobRequest = Mock(JobRequest)
        def jobId = UUID.randomUUID().toString()
        def selectionException = new ResourceSelectionException("some error")
        def scriptResult = Mock(ResourceSelectorScriptResult)
        def context = new CommandSelectionContext(
            jobId,
            jobRequest,
            true,
            commandClusters
        )

        ResourceSelectionResult<Command> result
        Set<Tag> expectedTags

        when: "Script returns no command"
        expectedTags = MetricsUtils.newSuccessTagsSet()
        expectedTags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_ID, "null"))
        result = this.commandSelector.select(context)

        then:
        1 * this.script.selectResource(context) >> scriptResult
        1 * scriptResult.getResource() >> Optional.empty()
        1 * scriptResult.getRationale() >> Optional.empty()
        1 * this.registry.timer(
            ScriptCommandSelectorImpl.SELECT_TIMER_NAME,
            {
                it == expectedTags
            }
        ) >> this.timer
        1 * this.timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        result.getSelectorClass() == this.commandSelector.getClass()
        !result.getSelectedResource().isPresent()
        result.getSelectionRationale().orElse(UUID.randomUUID().toString()) == "Script returned no command, no preference"

        when: "Script throws exception"
        expectedTags = MetricsUtils.newFailureTagsSetForException(selectionException)
        this.commandSelector.select(context)

        then:
        1 * this.script.selectResource(context) >> { throw selectionException }
        1 * this.registry.timer(
            ScriptCommandSelectorImpl.SELECT_TIMER_NAME,
            { it == expectedTags }
        ) >> this.timer
        1 * this.timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        thrown(ResourceSelectionException)

        when: "Script selects a command"
        expectedTags = MetricsUtils.newSuccessTagsSet()
        expectedTags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_ID, "command 1 id"))
        expectedTags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_NAME, "command 1 name"))
        result = this.commandSelector.select(context)

        then:
        1 * this.script.selectResource(context) >> scriptResult
        1 * scriptResult.getResource() >> Optional.of(command1)
        1 * scriptResult.getRationale() >> Optional.of("Good to go!")
        1 * command1.getId() >> "command 1 id"
        1 * command1.getMetadata() >> command1Metadata
        1 * command1Metadata.getName() >> "command 1 name"
        1 * this.registry.timer(
            ScriptCommandSelectorImpl.SELECT_TIMER_NAME,
            { it == expectedTags }
        ) >> this.timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        result.getSelectorClass() == this.commandSelector.getClass()
        result.getSelectedResource().orElse(null) == command1
        result.getSelectionRationale().orElse(UUID.randomUUID().toString()) == "Good to go!"
    }
}
