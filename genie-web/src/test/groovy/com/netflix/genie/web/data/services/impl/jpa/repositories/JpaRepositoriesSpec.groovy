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
package com.netflix.genie.web.data.services.impl.jpa.repositories

import spock.lang.Specification

/**
 * Specifications for {@link JpaRepositories}.
 *
 * @author tgianos
 */
class JpaRepositoriesSpec extends Specification {

    def "can construct"() {
        def applicationRepo = Mock(JpaApplicationRepository)
        def clusterRepo = Mock(JpaClusterRepository)
        def commandRepo = Mock(JpaCommandRepository)
        def criterionRepo = Mock(JpaCriterionRepository)
        def fileRepo = Mock(JpaFileRepository)
        def jobRepo = Mock(JpaJobRepository)
        def tagRepo = Mock(JpaTagRepository)

        when:
        def repositories = new JpaRepositories(
            applicationRepo,
            clusterRepo,
            commandRepo,
            criterionRepo,
            fileRepo,
            jobRepo,
            tagRepo
        )

        then:
        repositories.getApplicationRepository() == applicationRepo
        repositories.getClusterRepository() == clusterRepo
        repositories.getCommandRepository() == commandRepo
        repositories.getCriterionRepository() == criterionRepo
        repositories.getFileRepository() == fileRepo
        repositories.getJobRepository() == jobRepo
        repositories.getTagRepository() == tagRepo
    }
}
