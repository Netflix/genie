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
package com.netflix.genie.common.dto

import spock.lang.Specification

/**
 * Specifications for {@link Runtime}.
 *
 * @author tgianos
 */
class RuntimeSpec extends Specification {

    def "can create and validate"() {
        when:
        def runtime0 = new Runtime.Builder().withResources(resources).withImages(images).build()
        def runtime1 = new Runtime.Builder().withResources(resources).withImages(images).build()
        def runtime2 = new Runtime.Builder()
            .withResources(new RuntimeResources.Builder().withNetworkMbps(100_000L).build())
            .withImages(
                [
                    (UUID.randomUUID().toString()): new ContainerImage.Builder().withName(UUID.randomUUID().toString()).build()
                ]
            )
            .build()

        then:
        runtime0.getResources() == (resources == null ? new RuntimeResources.Builder().build() : resources)
        runtime0.getImages() == (images == null ? new HashMap<String, ContainerImage>() : images)
        runtime0 == runtime1
        runtime0 != runtime2
        runtime0.hashCode() == runtime1.hashCode()
        runtime0.hashCode() != runtime2.hashCode()
        runtime0.toString() == runtime1.toString()
        runtime0.toString() != runtime2.toString()

        where:
        resources                                                   | images
        new RuntimeResources.Builder().withCpu(10).build()          | [genie: new ContainerImage.Builder().withName(UUID.randomUUID().toString()).build()]
        null                                                        | [python: new ContainerImage.Builder().withTag("latest.release").build()]
        new RuntimeResources.Builder().withMemoryMb(3_243L).build() | null
        null                                                        | null
    }
}
