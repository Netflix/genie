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
package com.netflix.genie.common.external.dtos.v4

import com.google.common.collect.Sets
import spock.lang.Specification

/**
 * Specifications for {@link ResolvedResources}.
 *
 * @author tgianos
 */
class ResolvedResourcesSpec extends Specification {

    def "Can construct"() {
        def criterion = new Criterion.Builder().withId(UUID.randomUUID().toString()).build()
        def resources = Sets.newHashSet(UUID.randomUUID().toString())

        when:
        def resolved = new ResolvedResources<String>(criterion, resources)

        then:
        resolved.getCriterion() == criterion
        resolved.getResources() == resources
    }

    def "Equals, hashCode, toString work as expected"() {
        def criterion0 = new Criterion.Builder().withId(UUID.randomUUID().toString()).build()
        def criterion1 = new Criterion.Builder().withName(UUID.randomUUID().toString()).build()

        def resources0 = Sets.newHashSet(0, 1, 2)
        def resources1 = Sets.newHashSet(3, 4, 5)

        when:
        def resolved0 = new ResolvedResources<Integer>(criterion0, resources0)
        def resolved1 = new ResolvedResources<Integer>(criterion0, resources0)
        def resolved2 = new ResolvedResources<Integer>(criterion0, resources1)
        def resolved3 = new ResolvedResources<Integer>(criterion1, resources0)

        then:
        resolved0 == resolved1
        resolved0.hashCode() == resolved1.hashCode()
        resolved0.toString() == resolved1.toString()

        resolved0 != resolved2
        resolved0 != resolved3

        resolved0.hashCode() != resolved2.hashCode()
        resolved0.hashCode() != resolved2.hashCode()

        resolved0.toString() != resolved2.toString()
        resolved0.toString() != resolved2.toString()
    }

    def "Attempting to modify the resources throws exception"() {
        def resolved = new ResolvedResources<Double>(
            new Criterion.Builder().withVersion(UUID.randomUUID().toString()).build(),
            Sets.newHashSet(0.0d, 24312.13d)
        )

        when:
        resolved.getResources().add(3243d)

        then:
        thrown(UnsupportedOperationException.class)
    }
}
