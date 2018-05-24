/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.common.internal.dto.v4

import com.netflix.genie.common.dto.ApplicationStatus
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

import java.time.Instant

/**
 * Specifications for the {@link Application} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class ApplicationSpec extends Specification {

    def "Can build immutable application resource"() {
        def metadata = new ApplicationMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ApplicationStatus.INACTIVE
        )
                .withType(UUID.randomUUID().toString())
                .build()
        def id = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def created = Instant.now()
        def updated = Instant.now()
        Application application

        when:
        application = new Application(
                id,
                created,
                updated,
                resources,
                metadata
        )

        then:
        application.getId() == id
        application.getCreated() == created
        application.getUpdated() == updated
        application.getResources() == resources
        application.getMetadata() == metadata

        when:
        application = new Application(
                id,
                created,
                updated,
                null,
                metadata
        )

        then:
        application.getId() == id
        application.getCreated() == created
        application.getUpdated() == updated
        application.getResources() == new ExecutionEnvironment(null, null, null)
        application.getMetadata() == metadata
    }

    def "Test equals"() {
        def base = createApplication()
        Object comparable

        when:
        comparable = base

        then:
        base == comparable

        when:
        comparable = null

        then:
        base != comparable

        when:
        comparable = new Application(
                UUID.randomUUID().toString(),
                Instant.now(),
                Instant.now(),
                Mock(ExecutionEnvironment),
                Mock(ApplicationMetadata)
        )

        then:
        base != comparable

        when:
        comparable = "I'm definitely not the right type of object"

        then:
        base != comparable

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ApplicationStatus.INACTIVE
        def id = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        def baseMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        base = new Application(id, created, updated, null, baseMetadata)
        comparable = new Application(id, created, updated, null, comparableMetadata)

        then:
        base == comparable
    }

    def "Test hashCode"() {
        Application one
        Application two

        when:
        one = createApplication()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createApplication()
        two = createApplication()

        then:
        one.hashCode() != two.hashCode()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ApplicationStatus.INACTIVE
        def id = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        def baseMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        one = new Application(id, created, updated, null, baseMetadata)
        two = new Application(id, created, updated, null, comparableMetadata)

        then:
        one.hashCode() == two.hashCode()
    }

    def "toString is consistent"() {
        Application one
        Application two

        when:
        one = createApplication()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createApplication()
        two = createApplication()

        then:
        one.toString() != two.toString()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ApplicationStatus.INACTIVE
        def id = UUID.randomUUID().toString()
        def created = Instant.now()
        def updated = Instant.now()
        def baseMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        one = new Application(id, created, updated, null, baseMetadata)
        two = new Application(id, created, updated, null, comparableMetadata)

        then:
        one.toString() == two.toString()
    }

    Application createApplication() {
        def metadata = new ApplicationMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ApplicationStatus.INACTIVE
        )
                .withType(UUID.randomUUID().toString())
                .build()
        def id = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def created = Instant.now()
        def updated = Instant.now()
        return new Application(id, created, updated, resources, metadata)
    }
}
