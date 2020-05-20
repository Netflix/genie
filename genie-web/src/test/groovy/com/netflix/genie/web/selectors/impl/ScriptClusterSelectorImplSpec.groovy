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
import com.netflix.genie.common.external.dtos.v4.Cluster
import com.netflix.genie.common.external.dtos.v4.ClusterMetadata
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.web.dtos.ResourceSelectionResult
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException
import com.netflix.genie.web.scripts.ClusterSelectorManagedScript
import com.netflix.genie.web.scripts.ResourceSelectorScriptResult
import com.netflix.genie.web.selectors.ClusterSelectionContext
import com.netflix.genie.web.util.MetricsConstants
import com.netflix.genie.web.util.MetricsUtils
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * Specifications for the {@link ScriptClusterSelectorImpl} class.
 */
@SuppressWarnings("GroovyAccessibility")
class ScriptClusterSelectorImplSpec extends Specification {

    ClusterSelectorManagedScript script
    MeterRegistry registry
    ScriptClusterSelectorImpl scriptClusterSelector
    Timer timer

    def setup() {
        this.timer = Mock(Timer)
        this.script = Mock(ClusterSelectorManagedScript)
        this.registry = Mock(MeterRegistry)
        this.scriptClusterSelector = new ScriptClusterSelectorImpl(this.script, this.registry)
    }

    def "can select a cluster"() {
        Cluster cluster1 = Mock(Cluster)
        Cluster cluster2 = Mock(Cluster)
        Set<Cluster> clusters = Sets.newHashSet(cluster1, cluster2)
        ClusterMetadata cluster2metadata = Mock(ClusterMetadata)
        JobRequest jobRequest = Mock(JobRequest)
        String jobId = UUID.randomUUID().toString()
        Throwable executionException = new ScriptExecutionException("some error")
        ClusterSelectionContext context = new ClusterSelectionContext(
            jobId,
            jobRequest,
            true,
            null,
            clusters
        )

        ResourceSelectionResult<Cluster> result
        Set<Tag> expectedTags
        ResourceSelectorScriptResult<Cluster> scriptResult = Mock(ResourceSelectorScriptResult)

        when: "Script returns null"
        expectedTags = MetricsUtils.newSuccessTagsSet()
        expectedTags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, ScriptClusterSelectorImpl.NULL_TAG))
        result = this.scriptClusterSelector.select(context)

        then:
        1 * this.script.selectResource(context) >> scriptResult
        1 * scriptResult.getResource() >> Optional.empty()
        1 * scriptResult.getRationale() >> Optional.empty()
        1 * this.registry.timer(
            ScriptClusterSelectorImpl.SELECT_TIMER_NAME,
            {
                it == expectedTags
            }
        ) >> this.timer
        1 * this.timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        !result.getSelectedResource().isPresent()
        result.getSelectionRationale().orElse(null) == ScriptClusterSelectorImpl.NULL_RATIONALE
        result.getSelectorClass() == ScriptClusterSelectorImpl.class

        when: "Script throws"
        expectedTags = MetricsUtils.newFailureTagsSetForException(executionException)
        this.scriptClusterSelector.select(context)

        then:
        1 * this.script.selectResource(context) >> { throw executionException }
        1 * this.registry.timer(
            ScriptClusterSelectorImpl.SELECT_TIMER_NAME,
            { it == expectedTags }
        ) >> this.timer
        1 * this.timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        thrown(ResourceSelectionException)

        when: "Script selects cluster"
        expectedTags = MetricsUtils.newSuccessTagsSet()
        expectedTags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, "cluster2"))
        expectedTags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_NAME, "Cluster 2"))
        result = this.scriptClusterSelector.select(context)

        then:
        1 * this.script.selectResource(context) >> scriptResult
        1 * scriptResult.getResource() >> Optional.of(cluster2)
        1 * scriptResult.getRationale() >> Optional.of("Cluster 2 was good")
        1 * cluster2.getId() >> "cluster2"
        1 * cluster2.getMetadata() >> cluster2metadata
        1 * cluster2metadata.getName() >> "Cluster 2"
        1 * this.registry.timer(
            ScriptClusterSelectorImpl.SELECT_TIMER_NAME,
            { it == expectedTags }
        ) >> this.timer
        1 * this.timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        result.getSelectedResource().orElse(null) == cluster2
        result.getSelectionRationale().orElse(null) == "Cluster 2 was good"
        result.getSelectorClass() == ScriptClusterSelectorImpl.class
    }
}
