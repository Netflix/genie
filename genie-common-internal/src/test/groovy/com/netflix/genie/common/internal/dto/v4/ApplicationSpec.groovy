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
import spock.lang.Specification

import java.time.Instant

/**
 * Specifications for the {@link Application} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
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
}
