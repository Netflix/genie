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
package com.netflix.genie.web.data.services

import spock.lang.Specification

/**
 * Specifications for the {@link DataServices} class.
 *
 * @author tgianos
 */
class DataServicesSpec extends Specification {

    def "can build properly"() {
        def agentConnectionService = Mock(AgentConnectionPersistenceService)
        def applicationService = Mock(ApplicationPersistenceService)
        def clusterService = Mock(ClusterPersistenceService)
        def commandService = Mock(CommandPersistenceService)
        def fileService = Mock(FilePersistenceService)
        def jobService = Mock(JobPersistenceService)
        def jobSearchService = Mock(JobSearchService)
        def tagService = Mock(TagPersistenceService)

        when:
        def services = new DataServices(
            agentConnectionService,
            applicationService,
            clusterService,
            commandService,
            fileService,
            jobService,
            jobSearchService,
            tagService
        )

        then:
        services.getAgentConnectionPersistenceService() == agentConnectionService
        services.getApplicationPersistenceService() == applicationService
        services.getClusterPersistenceService() == clusterService
        services.getCommandPersistenceService() == commandService
        services.getFilePersistenceService() == fileService
        services.getJobPersistenceService() == jobService
        services.getJobSearchService() == jobSearchService
        services.getTagPersistenceService() == tagService
    }
}
