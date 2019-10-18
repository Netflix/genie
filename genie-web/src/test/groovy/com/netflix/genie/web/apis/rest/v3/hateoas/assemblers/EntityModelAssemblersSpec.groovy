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
 * Specifications for {@link EntityModelAssemblers}.
 *
 * @author tgianos
 */
class EntityModelAssemblersSpec extends Specification {

    def "Can build"() {
        def applications = Mock(ApplicationModelAssembler)
        def clusters = Mock(ClusterModelAssembler)
        def commands = Mock(CommandModelAssembler)
        def jobExecutions = Mock(JobExecutionModelAssembler)
        def jobMetadata = Mock(JobMetadataModelAssembler)
        def jobRequests = Mock(JobRequestModelAssembler)
        def jobs = Mock(JobModelAssembler)
        def jobSearches = Mock(JobSearchResultModelAssembler)
        def root = Mock(RootModelAssembler)

        when:
        def assemblers = new EntityModelAssemblers(
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
        assemblers.getApplicationModelAssembler() == applications
        assemblers.getClusterModelAssembler() == clusters
        assemblers.getCommandModelAssembler() == commands
        assemblers.getJobExecutionModelAssembler() == jobExecutions
        assemblers.getJobMetadataModelAssembler() == jobMetadata
        assemblers.getJobRequestModelAssembler() == jobRequests
        assemblers.getJobModelAssembler() == jobs
        assemblers.getJobSearchResultModelAssembler() == jobSearches
        assemblers.getRootModelAssembler() == root
    }
}
