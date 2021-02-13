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

import com.google.common.collect.Sets
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata
import com.netflix.genie.web.agent.launchers.AgentLauncher
import com.netflix.genie.web.dtos.ResolvedJob
import spock.lang.Specification

class AgentLauncherSelectionContextSpec extends Specification {

    def "Constructor"() {
        def jobId = UUID.randomUUID().toString()
        def jobRequest = Mock(JobRequest)
        def jobRequestMetadata = Mock(JobRequestMetadata)
        def launchers = Sets.newHashSet(Mock(AgentLauncher), Mock(AgentLauncher))
        def resolvedJob = Mock(ResolvedJob)

        when:
        def context = new AgentLauncherSelectionContext(
            jobId,
            jobRequest,
            jobRequestMetadata,
            resolvedJob,
            launchers
        )

        then:
        context.getJobRequest() == jobRequest
        context.getJobRequestMetadata() == jobRequestMetadata
        context.getJobId() == jobId
        context.getAgentLaunchers() == launchers
        context.getResolvedJob() == resolvedJob
        context.getResources() == launchers
    }
}
