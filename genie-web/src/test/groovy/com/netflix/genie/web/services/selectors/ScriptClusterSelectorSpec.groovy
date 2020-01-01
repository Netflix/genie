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
package com.netflix.genie.web.services.selectors

import com.google.common.collect.Sets
import com.netflix.genie.common.dto.JobRequest
import com.netflix.genie.common.external.dtos.v4.Cluster
import com.netflix.genie.common.external.dtos.v4.ClusterMetadata
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException
import com.netflix.genie.web.scripts.ClusterSelectorScript
import com.netflix.genie.web.util.MetricsConstants
import com.netflix.genie.web.util.MetricsUtils
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * Specifications for the ScriptClusterSelector class.
 */
class ScriptClusterSelectorSpec extends Specification {

    ClusterSelectorScript script
    MeterRegistry registry
    ScriptClusterSelector scriptClusterSelector
    Timer timer

    def setup() {
        this.timer = Mock(Timer)
        this.script = Mock(ClusterSelectorScript)
        this.registry = Mock(MeterRegistry)
        this.scriptClusterSelector = new ScriptClusterSelector(script, registry)
    }

    def "selectCluster"() {

        Cluster cluster1 = Mock(Cluster)
        Cluster cluster2 = Mock(Cluster)
        Cluster cluster3 = Mock(Cluster)
        Set<Cluster> clusters = Sets.newHashSet(cluster1, cluster2)
        ClusterMetadata cluster2metadata = Mock(ClusterMetadata)
        JobRequest jobRequest = Mock(JobRequest)
        Throwable executionException = new ScriptExecutionException("some error")
        Throwable notFoundException = new GenieClusterNotFoundException("some error")

        Cluster selectedCluster
        Set<Tag> expectedTags

        when: "Script returns null"
        expectedTags = MetricsUtils.newSuccessTagsSet()
        expectedTags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, "null"))
        selectedCluster = this.scriptClusterSelector.selectCluster(clusters, jobRequest)

        then:
        1 * script.selectCluster(jobRequest, clusters) >> null
        1 * registry.timer(
            ScriptClusterSelector.SELECT_TIMER_NAME,
            {
                it == expectedTags
            }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        selectedCluster == null

        when: "Script throws"
        expectedTags = MetricsUtils.newFailureTagsSetForException(executionException)
        selectedCluster = this.scriptClusterSelector.selectCluster(clusters, jobRequest)

        then:
        1 * script.selectCluster(jobRequest, clusters) >> { throw executionException }
        1 * registry.timer(
            ScriptClusterSelector.SELECT_TIMER_NAME,
            { it == expectedTags }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        selectedCluster == null

        when: "Script selects cluster"
        expectedTags = MetricsUtils.newSuccessTagsSet()
        expectedTags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, "cluster2"))
        expectedTags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_NAME, "Cluster 2"))
        selectedCluster = this.scriptClusterSelector.selectCluster(clusters, jobRequest)

        then:
        1 * script.selectCluster(jobRequest, clusters) >> cluster2
        1 * cluster2.getId() >> "cluster2"
        1 * cluster2.getMetadata() >> cluster2metadata
        1 * cluster2metadata.getName() >> "Cluster 2"
        1 * registry.timer(
            ScriptClusterSelector.SELECT_TIMER_NAME,
            { it == expectedTags }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        selectedCluster == cluster2
    }
}
