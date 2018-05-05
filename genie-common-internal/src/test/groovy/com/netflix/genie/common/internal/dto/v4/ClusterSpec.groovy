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

import com.netflix.genie.common.dto.ClusterStatus
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
class ClusterSpec extends Specification {

    def "Can build immutable cluster resource"() {
        def metadata = new ClusterMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.UP
        ).build()
        def id = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def created = Instant.now()
        def updated = Instant.now()
        Cluster cluster

        when:
        cluster = new Cluster(
                id,
                created,
                updated,
                resources,
                metadata
        )

        then:
        cluster.getId() == id
        cluster.getCreated() == created
        cluster.getUpdated() == updated
        cluster.getResources() == resources
        cluster.getMetadata() == metadata

        when:
        cluster = new Cluster(
                id,
                created,
                updated,
                null,
                metadata
        )

        then:
        cluster.getId() == id
        cluster.getCreated() == created
        cluster.getUpdated() == updated
        cluster.getResources() == new ExecutionEnvironment(null, null, null)
        cluster.getMetadata() == metadata
    }
}
