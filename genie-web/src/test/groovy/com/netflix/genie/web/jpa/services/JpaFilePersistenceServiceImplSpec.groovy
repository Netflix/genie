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
import com.netflix.genie.web.jpa.entities.FileEntity
import com.netflix.genie.web.jpa.repositories.JpaFileRepository
import org.junit.experimental.categories.Category
import org.springframework.dao.DuplicateKeyException
import spock.lang.Specification

/**
 * Unit tests for JpaFilePersistenceServiceImpl.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(UnitTest.class)
class JpaFilePersistenceServiceImplSpec extends Specification {
    def fileRepository = Mock(JpaFileRepository)
    def service = new JpaFilePersistenceServiceImpl(this.fileRepository)

    def "If file exists no creation is attempted"() {
        def file = UUID.randomUUID().toString()

        when:
        this.service.createFileIfNotExists(file)

        then:
        1 * this.fileRepository.existsByFile(file) >> true
        0 * this.fileRepository.saveAndFlush(_ as FileEntity)
    }

    def "If file doesn't exist creation is attempted"() {
        def file = UUID.randomUUID().toString()

        when:
        this.service.createFileIfNotExists(file)

        then:
        1 * this.fileRepository.existsByFile(file) >> false
        1 * this.fileRepository.saveAndFlush(_ as FileEntity) >> {
            final FileEntity fileEntity ->
                assert fileEntity.getFile() == file
                fileEntity
        }
    }

    def "If a file was created between previous check and create will still succeed on duplicate key exception"() {
        def file = UUID.randomUUID().toString()

        when:
        this.service.createFileIfNotExists(file)

        then:
        1 * this.fileRepository.existsByFile(file) >> false
        1 * this.fileRepository.saveAndFlush(_ as FileEntity) >> {
            final FileEntity fileEntity ->
                assert fileEntity.getFile() == file
                throw new DuplicateKeyException("Duplicate key")
        }
    }
}
