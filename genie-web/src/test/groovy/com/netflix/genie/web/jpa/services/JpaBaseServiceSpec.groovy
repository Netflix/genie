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

import com.google.common.collect.Sets
import com.netflix.genie.common.exceptions.GenieNotFoundException
import com.netflix.genie.common.exceptions.GenieServerException
import com.netflix.genie.web.jpa.entities.TagEntity
import com.netflix.genie.web.jpa.repositories.JpaFileRepository
import com.netflix.genie.web.jpa.repositories.JpaTagRepository
import com.netflix.genie.web.services.FileService
import com.netflix.genie.web.services.TagService
import spock.lang.Specification

/**
 * Specifications for JpaBaseService class.
 *
 * @author tgianos
 * @since 3.3.0
 */
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

    def "Can set the final tags for an entity"() {
        def id = UUID.randomUUID().toString()
        def name = UUID.randomUUID().toString()

        def genieIdTag = JpaBaseService.GENIE_ID_TAG_NAMESPACE + id
        def genieNameTag = JpaBaseService.GENIE_NAME_TAG_NAMESPACE + name

        def idTagEntity = new TagEntity()
        idTagEntity.setTag(genieIdTag)

        def nameTagEntity = new TagEntity()
        nameTagEntity.setTag(genieNameTag)

        def tagEntity1 = new TagEntity()
        tagEntity1.setTag(UUID.randomUUID().toString())

        def tagEntity2 = new TagEntity()
        tagEntity2.setTag(UUID.randomUUID().toString())

        def nameTagEntity2 = new TagEntity()
        nameTagEntity2.setTag(JpaBaseService.GENIE_NAME_TAG_NAMESPACE + UUID.randomUUID().toString())

        def tagService = Mock(TagService.class) {
            3 * createTagIfNotExists(genieIdTag)
            2 * createTagIfNotExists(genieNameTag)
        }
        def jpaTagRepository = Mock(JpaTagRepository.class) {
            3 * findByTag(genieIdTag) >> Optional.of(idTagEntity)
            2 * findByTag(genieNameTag) >> Optional.of(nameTagEntity)
        }
        def service = new JpaBaseService(
                tagService,
                jpaTagRepository,
                Mock(FileService),
                Mock(JpaFileRepository)
        )

        def expectedTags = Sets.newHashSet(idTagEntity, nameTagEntity, tagEntity1, tagEntity2)
        def noGenieTags = Sets.newHashSet(tagEntity1, tagEntity2)
        def nameTags = Sets.newHashSet(nameTagEntity, tagEntity1, tagEntity2)
        def twoNameTags = Sets.newHashSet(nameTagEntity, nameTagEntity2, tagEntity1, tagEntity2)
        def idTags = Sets.newHashSet(idTagEntity, tagEntity1, tagEntity2)

        when:
        service.setFinalTags(noGenieTags, id, name)

        then:
        noGenieTags == expectedTags

        when:
        service.setFinalTags(nameTags, id, name)

        then:
        nameTags == expectedTags

        when:
        service.setFinalTags(twoNameTags, id, name)

        then:
        twoNameTags == expectedTags

        when:
        service.setFinalTags(idTags, id, name)

        then:
        idTags == expectedTags
    }

    def "When there are two id tags trying to set final tags throws an exception"() {
        def service = new JpaBaseService(
                Mock(TagService),
                Mock(JpaTagRepository),
                Mock(FileService),
                Mock(JpaFileRepository)
        )

        def idTag1 = JpaBaseService.GENIE_ID_TAG_NAMESPACE + UUID.randomUUID().toString()
        def idTag2 = JpaBaseService.GENIE_ID_TAG_NAMESPACE + UUID.randomUUID().toString()

        def idEntity1 = new TagEntity()
        idEntity1.setTag(idTag1)

        def idEntity2 = new TagEntity()
        idEntity2.setTag(idTag2)

        when:
        service.setFinalTags(
                Sets.newHashSet(idEntity1, idEntity2),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )

        then:
        thrown(GenieServerException)
    }
}
