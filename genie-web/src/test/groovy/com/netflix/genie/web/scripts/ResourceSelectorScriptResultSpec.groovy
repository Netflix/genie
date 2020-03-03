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
package com.netflix.genie.web.scripts

import spock.lang.Specification

/**
 * Specifications for {@link ResourceSelectorScriptResult}.
 *
 * @author tgianos
 */
class ResourceSelectorScriptResultSpec extends Specification {

    def "can build"() {
        def builder = new ResourceSelectorScriptResult.Builder<String>()

        when:
        builder.withResource(null)
        builder.withRationale(null)
        def result = builder.build()

        then:
        !result.getResource().isPresent()
        !result.getRationale().isPresent()

        when:
        def resource = UUID.randomUUID().toString()
        def rationale = UUID.randomUUID().toString()
        builder.withResource(resource)
        builder.withRationale(rationale)
        result = builder.build()

        then:
        result.getResource().orElseThrow(IllegalArgumentException.&new) == resource
        result.getRationale().orElseThrow(IllegalArgumentException.&new) == rationale
    }

    def "equals, hashcode and toString behave as expected"() {
        when:
        def one = new ResourceSelectorScriptResult.Builder<String>()
            .withResource(UUID.randomUUID().toString())
            .withRationale(UUID.randomUUID().toString())
            .build()
        def two = new ResourceSelectorScriptResult.Builder<String>()
            .withResource(one.getResource().orElse(UUID.randomUUID().toString()))
            .withRationale(one.getRationale().orElse(UUID.randomUUID().toString()))
            .build()
        def three = new ResourceSelectorScriptResult.Builder<String>().build()

        then:
        one == two
        one != three

        one.hashCode() == two.hashCode()
        one.hashCode() != three.hashCode()

        one.toString() == two.toString()
        one.toString() != three.toString()
    }
}
