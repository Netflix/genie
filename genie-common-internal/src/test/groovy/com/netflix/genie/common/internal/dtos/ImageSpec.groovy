/*
 *
 *  Copyright 2022 Netflix, Inc.
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
package com.netflix.genie.common.internal.dtos

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link Image}.
 *
 * @author tgianos
 */
class ImageSpec extends Specification {

    @Unroll
    def "can validate for #name, #tag, #arguments"() {
        when:
        def image0 = new Image.Builder()
            .withName(name)
            .withTag(tag)
            .withArguments(arguments)
            .build()

        def image1 = new Image.Builder()
            .withName(name)
            .withTag(tag)
            .withArguments(arguments)
            .build()

        def image2 = new Image.Builder()
            .withName(name == null ? UUID.randomUUID().toString() : name + "blah")
            .withTag(tag == null ? UUID.randomUUID().toString() : tag + "blah")
            .withArguments([UUID.randomUUID().toString()])
            .build()

        then:
        image0.getName() == Optional.ofNullable(name)
        image0.getTag() == Optional.ofNullable(tag)
        image0.getArguments() == (arguments == null ? new ArrayList<String>() : arguments)

        image0 == image1
        image0 != image2

        image0.hashCode() == image1.hashCode()
        image0.hashCode() != image2.hashCode()

        image0.toString() == image1.toString()
        image0.toString() != image2.toString()

        where:
        name                         | tag                          | arguments
        UUID.randomUUID().toString() | UUID.randomUUID().toString() | [UUID.randomUUID().toString()]
        UUID.randomUUID().toString() | null                         | null
        null                         | UUID.randomUUID().toString() | null
        null                         | null                         | [UUID.randomUUID().toString(), UUID.randomUUID().toString()]
        null                         | null                         | null
    }
}
