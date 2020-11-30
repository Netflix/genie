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
package com.netflix.genie.web.scripts

import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import com.netflix.genie.common.external.dtos.v4.Cluster
import com.netflix.genie.common.external.dtos.v4.Command
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.selectors.AgentLauncherSelectionContext
import com.netflix.genie.web.selectors.ClusterSelectionContext
import com.netflix.genie.web.selectors.CommandSelectionContext
import spock.lang.Specification

/**
 * Specifications for {@link GroovyScriptUtils}.
 *
 * @author tgianos
 */
class GroovyScriptUtilsSpec extends Specification {

    Binding scriptBinding

    def setup() {
        this.scriptBinding = new Binding()
    }

    def "Can get command selection context"() {
        when:
        GroovyScriptUtils.getCommandSelectionContext(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        this.scriptBinding.setVariable(ResourceSelectorScript.CONTEXT_BINDING, 1)
        GroovyScriptUtils.getCommandSelectionContext(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        def expectedContext = new CommandSelectionContext(
            UUID.randomUUID().toString(),
            Mock(JobRequest),
            true,
            [
                (Mock(Command)): Sets.newHashSet(Mock(Cluster)),
                (Mock(Command)): Sets.newHashSet(Mock(Cluster), Mock(Cluster))
            ]
        )
        this.scriptBinding.setVariable(ResourceSelectorScript.CONTEXT_BINDING, expectedContext)
        def context = GroovyScriptUtils.getCommandSelectionContext(this.scriptBinding)

        then:
        context == expectedContext
    }

    def "Can get cluster selection context"() {
        when:
        GroovyScriptUtils.getClusterSelectionContext(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        this.scriptBinding.setVariable(ResourceSelectorScript.CONTEXT_BINDING, 1)
        GroovyScriptUtils.getClusterSelectionContext(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        def expectedContext = new ClusterSelectionContext(
            UUID.randomUUID().toString(),
            Mock(JobRequest),
            true,
            Mock(Command),
            Sets.newHashSet(Mock(Cluster))
        )
        this.scriptBinding.setVariable(ResourceSelectorScript.CONTEXT_BINDING, expectedContext)
        def context = GroovyScriptUtils.getClusterSelectionContext(this.scriptBinding)

        then:
        context == expectedContext
    }

    def "Can get agent launcher selection context"() {
        when:
        GroovyScriptUtils.getAgentLauncherSelectionContext(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        this.scriptBinding.setVariable(ResourceSelectorScript.CONTEXT_BINDING, 1)
        GroovyScriptUtils.getAgentLauncherSelectionContext(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        def expectedContext = new AgentLauncherSelectionContext(
            UUID.randomUUID().toString(),
            Mock(JobRequest),
            Mock(JobRequestMetadata),
            Mock(ResolvedJob),
            Lists.newArrayList()
        )
        this.scriptBinding.setVariable(ResourceSelectorScript.CONTEXT_BINDING, expectedContext)
        def context = GroovyScriptUtils.getAgentLauncherSelectionContext(this.scriptBinding)

        then:
        context == expectedContext
    }


    def "Can get properties"() {
        Map<String, String> propertiesMap
        Map<String, String> expectedPropertiesMap = Maps.newHashMap()

        when:
        propertiesMap = GroovyScriptUtils.getProperties(this.scriptBinding)

        then:
        propertiesMap.isEmpty()

        when:
        this.scriptBinding.setVariable(ResourceSelectorScript.PROPERTIES_MAP_BINDING, 1)
        propertiesMap = GroovyScriptUtils.getProperties(this.scriptBinding)

        then:
        propertiesMap.isEmpty()

        when:
        this.scriptBinding.setVariable(ResourceSelectorScript.PROPERTIES_MAP_BINDING, new Object())
        propertiesMap = GroovyScriptUtils.getProperties(this.scriptBinding)

        then:
        propertiesMap.isEmpty()

        when:
        expectedPropertiesMap.put("Foo", "true")
        expectedPropertiesMap.put("Bar", "3.14")
        this.scriptBinding.setVariable(ResourceSelectorScript.PROPERTIES_MAP_BINDING, expectedPropertiesMap)
        propertiesMap = GroovyScriptUtils.getProperties(this.scriptBinding)

        then:
        propertiesMap == expectedPropertiesMap
    }

    def "Can get clusters"() {
        when:
        GroovyScriptUtils.getClusters(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        this.scriptBinding.setVariable(ClusterSelectorManagedScript.CLUSTERS_BINDING, Lists.newArrayList())
        GroovyScriptUtils.getClusters(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        this.scriptBinding.setVariable(ClusterSelectorManagedScript.CLUSTERS_BINDING, Sets.newHashSet())
        GroovyScriptUtils.getClusters(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        this.scriptBinding.setVariable(
            ClusterSelectorManagedScript.CLUSTERS_BINDING,
            Sets.newHashSet(Mock(Cluster), "not a cluster")
        )
        GroovyScriptUtils.getClusters(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        def expectedClusters = Sets.newHashSet(Mock(Cluster), Mock(Cluster))
        this.scriptBinding.setVariable(ClusterSelectorManagedScript.CLUSTERS_BINDING, expectedClusters)
        def clusters = GroovyScriptUtils.getClusters(this.scriptBinding)

        then:
        clusters == expectedClusters
    }

    def "Can get commands"() {
        when:
        GroovyScriptUtils.getCommands(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        this.scriptBinding.setVariable(CommandSelectorManagedScript.COMMANDS_BINDING, Lists.newArrayList())
        GroovyScriptUtils.getCommands(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        this.scriptBinding.setVariable(CommandSelectorManagedScript.COMMANDS_BINDING, Sets.newHashSet())
        GroovyScriptUtils.getCommands(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        this.scriptBinding.setVariable(
            CommandSelectorManagedScript.COMMANDS_BINDING,
            Sets.newHashSet(Mock(Command), "not a command")
        )
        GroovyScriptUtils.getCommands(this.scriptBinding)

        then:
        thrown(IllegalArgumentException)

        when:
        def expectedCommands = Sets.newHashSet(Mock(Command), Mock(Command))
        this.scriptBinding.setVariable(CommandSelectorManagedScript.COMMANDS_BINDING, expectedCommands)
        def commands = GroovyScriptUtils.getCommands(this.scriptBinding)

        then:
        commands == expectedCommands
    }
}
