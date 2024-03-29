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

import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import com.netflix.genie.common.internal.dtos.Cluster
import com.netflix.genie.common.internal.dtos.Command
import com.netflix.genie.common.internal.dtos.JobRequest
import spock.lang.Specification

/**
 * Specifications for {@link CommandSelectionContext}.
 *
 * @author tgianos
 */
class CommandSelectionContextSpec extends Specification {

    def "is immutable bean"() {
        def jobId = UUID.randomUUID().toString()
        def jobRequest = Mock(JobRequest)
        def cluster0 = Mock(Cluster)
        def cluster1 = Mock(Cluster)
        def cluster2 = Mock(Cluster)
        Map<Command, Set<Cluster>> commandToClusters = ImmutableMap.of(
            Mock(Command),
            ImmutableSet.of(cluster0, cluster1, cluster2)
        )

        when:
        def context = new CommandSelectionContext(jobId, jobRequest, true, commandToClusters)

        then:
        noExceptionThrown()
        context.getJobId() == jobId
        context.getJobRequest() == jobRequest
        context.isApiJob()
        context.getCommandToClusters() == commandToClusters
        context.getResources() == commandToClusters.keySet()

        when:
        context.toString()

        then:
        noExceptionThrown()
    }
}
