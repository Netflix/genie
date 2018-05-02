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

import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.jpa.entities.TagEntity
import com.netflix.genie.web.jpa.repositories.JpaTagRepository
import org.junit.experimental.categories.Category
import org.springframework.dao.DuplicateKeyException
import spock.lang.Specification

/**
 * Unit tests for JpaTagPersistenceServiceImpl.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(UnitTest.class)
class JpaTagPersistenceServiceImplSpec extends Specification {
    def tagRepository = Mock(JpaTagRepository)
    def service = new JpaTagPersistenceServiceImpl(this.tagRepository)

    def "If tag exists no creation is attempted"() {
        def tag = UUID.randomUUID().toString()

        when:
        this.service.createTagIfNotExists(tag)

        then:
        1 * this.tagRepository.existsByTag(tag) >> true
        0 * this.tagRepository.saveAndFlush(_ as TagEntity)
    }

    def "If tag doesn't exist creation is attempted"() {
        def tag = UUID.randomUUID().toString()

        when:
        this.service.createTagIfNotExists(tag)

        then:
        1 * this.tagRepository.existsByTag(tag) >> false
        1 * this.tagRepository.saveAndFlush(_ as TagEntity) >> {
            final TagEntity tagEntity ->
                assert tagEntity.getTag() == tag
                tagEntity
        }
    }

    def "If a tag was created between previous check and create will still succeed on duplicate key exception"() {
        def tag = UUID.randomUUID().toString()

        when:
        this.service.createTagIfNotExists(tag)

        then:
        1 * this.tagRepository.existsByTag(tag) >>> false
        1 * this.tagRepository.saveAndFlush(_ as TagEntity) >> {
            final TagEntity tagEntity ->
                assert tagEntity.getTag() == tag
                throw new DuplicateKeyException("Duplicate key")
        }
    }
}
