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
package com.netflix.genie.web.selectors


import com.google.common.collect.ImmutableSet
import com.netflix.genie.common.external.dtos.v4.Cluster
import com.netflix.genie.common.external.dtos.v4.Command
import com.netflix.genie.common.external.dtos.v4.JobRequest
import spock.lang.Specification

/**
 * Specifications for {@link ClusterSelectionContext}.
 *
 * @author tgianos
 */
class ClusterSelectionContextSpec extends Specification {

    def "is immutable bean"() {
        def jobId = UUID.randomUUID().toString()
        def jobRequest = Mock(JobRequest)
        def cluster0 = Mock(Cluster)
        def cluster1 = Mock(Cluster)
        def cluster2 = Mock(Cluster)
        def command = Mock(Command)
        def clusters = ImmutableSet.of(cluster0, cluster1, cluster2)

        when:
        def context = new ClusterSelectionContext(jobId, jobRequest, true, null, clusters)

        then:
        noExceptionThrown()
        context.getJobId() == jobId
        context.getJobRequest() == jobRequest
        context.isApiJob()
        !context.getCommand().isPresent()
        context.getClusters() == clusters
        context.getResources() == clusters

        when:
        context.toString()

        then:
        noExceptionThrown()

        when:
        context = new ClusterSelectionContext(jobId, jobRequest, true, command, clusters)

        then:
        noExceptionThrown()
        context.getJobId() == jobId
        context.getJobRequest() == jobRequest
        context.isApiJob()
        context.getCommand().orElse(null) == command
        context.getClusters() == clusters
        context.getResources() == clusters

        when:
        context.toString()

        then:
        noExceptionThrown()
    }
}
