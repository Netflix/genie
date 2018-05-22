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
package com.netflix.genie.web.jpa.services

import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for JpaBaseService class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(UnitTest.class)
class JpaBaseServiceSpec extends Specification {

    def "Can't get file entity if doesn't exist"() {
        def fileService = Mock(JpaFilePersistenceService) {
            1 * createFileIfNotExists(_ as String)
            1 * getFile(_ as String) >> Optional.empty()
        }
        def service = new JpaBaseService(
                Mock(JpaTagPersistenceService),
                fileService,
                Mock(JpaApplicationRepository),
                Mock(JpaClusterRepository),
                Mock(JpaCommandRepository)
        )

        when:
        service.createAndGetFileEntity(UUID.randomUUID().toString())

        then:
        thrown(GenieRuntimeException)
    }

    def "Can't get tag entity if doesn't exist"() {
        def tagService = Mock(JpaTagPersistenceService) {
            1 * createTagIfNotExists(_ as String)
            1 * getTag(_ as String) >> Optional.empty()
        }
        def service = new JpaBaseService(
                tagService,
                Mock(JpaFilePersistenceService),
                Mock(JpaApplicationRepository),
                Mock(JpaClusterRepository),
                Mock(JpaCommandRepository)
        )

        when:
        service.createAndGetTagEntity(UUID.randomUUID().toString())

        then:
        thrown(GenieRuntimeException)
    }
}
