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

import com.netflix.genie.common.external.dtos.v4.Cluster
import com.netflix.genie.common.external.dtos.v4.ClusterMetadata
import com.netflix.genie.common.external.dtos.v4.ClusterStatus
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.web.selectors.ClusterSelectionContext

import java.time.Instant

def binding = this.getBinding()

ClusterSelectionContext context = GroovyScriptUtils.getClusterSelectionContext(binding)
String jobId = context.getJobId()
JobRequest jobRequest = context.getJobRequest()
Set<Cluster> clusters = context.getClusters()

Cluster selectedCluster = null
String rationale = null

switch (jobId) {
    case "0":
        selectedCluster = clusters.find({ it -> (it.getId() == "0") })
        rationale = "selected 0"
        break
    case "1":
        rationale = "Couldn't find anything"
        break
    case "2":
        // Pretend for some reason the script returns something not in the original set
        selectedCluster = new Cluster(
            UUID.randomUUID().toString(),
            Instant.now(),
            Instant.now(),
            null,
            new ClusterMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.UP
            ).build()
        )
        break
    case "3":
        break
    case "5":
        return jobRequest.getMetadata().getName()
    default:
        throw new Exception("uh oh")
}

return new ResourceSelectorScriptResult.Builder<Cluster>()
    .withResource(selectedCluster)
    .withRationale(rationale)
    .build()
