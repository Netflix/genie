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

import com.netflix.genie.common.exceptions.GenieNotFoundException
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.jpa.repositories.JpaFileRepository
import com.netflix.genie.web.jpa.repositories.JpaTagRepository
import com.netflix.genie.web.services.FileService
import com.netflix.genie.web.services.TagService
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
        def jpaFileRepository = Mock(JpaFileRepository) {
            1 * findByFile(_ as String) >> Optional.empty()
        }
        def fileService = Mock(FileService) {
            1 * createFileIfNotExists(_ as String)
        }
        def service = new JpaBaseService(
                Mock(TagService),
                Mock(JpaTagRepository),
                fileService,
                jpaFileRepository
        )

        when:
        service.createAndGetFileEntity(UUID.randomUUID().toString())

        then:
        thrown(GenieNotFoundException)
    }

    def "Can't get tag entity if doesn't exist"() {
        def jpaTagRepository = Mock(JpaTagRepository) {
            1 * findByTag(_ as String) >> Optional.empty()
        }
        def tagService = Mock(TagService) {
            1 * createTagIfNotExists(_ as String)
        }
        def service = new JpaBaseService(
                tagService,
                jpaTagRepository,
                Mock(FileService),
                Mock(JpaFileRepository)
        )

        when:
        service.createAndGetTagEntity(UUID.randomUUID().toString())

        then:
        thrown(GenieNotFoundException)
    }
}
