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
package com.netflix.genie.web.apis.rest.v3.hateoas.assemblers

import spock.lang.Specification

/**
 * Specifications for {@link ResourceAssemblers}.
 *
 * @author tgianos
 */
class ResourceAssemblersSpec extends Specification {

    def "Can build"() {
        def applications = Mock(ApplicationResourceAssembler)
        def clusters = Mock(ClusterResourceAssembler)
        def commands = Mock(CommandResourceAssembler)
        def jobExecutions = Mock(JobExecutionResourceAssembler)
        def jobMetadata = Mock(JobMetadataResourceAssembler)
        def jobRequests = Mock(JobRequestResourceAssembler)
        def jobs = Mock(JobResourceAssembler)
        def jobSearches = Mock(JobSearchResultResourceAssembler)
        def root = Mock(RootResourceAssembler)

        when:
        def assemblers = new ResourceAssemblers(
            applications,
            clusters,
            commands,
            jobExecutions,
            jobMetadata,
            jobRequests,
            jobs,
            jobSearches,
            root
        )

        then:
        assemblers.getApplicationResourceAssembler() == applications
        assemblers.getClusterResourceAssembler() == clusters
        assemblers.getCommandResourceAssembler() == commands
        assemblers.getJobExecutionResourceAssembler() == jobExecutions
        assemblers.getJobMetadataResourceAssembler() == jobMetadata
        assemblers.getJobRequestResourceAssembler() == jobRequests
        assemblers.getJobResourceAssembler() == jobs
        assemblers.getJobSearchResultResourceAssembler() == jobSearches
        assemblers.getRootResourceAssembler() == root
    }
}
