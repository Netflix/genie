/*
 *
 *  Copyright 2019 Netflix, Inc.
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

import com.fasterxml.jackson.databind.JsonNode
import spock.lang.Specification

/**
 * Specifications for {@link JobEnvironment}.
 *
 * @author tgianos
 */
class JobEnvironmentSpec extends Specification {

    def "Generated instances are immutable and correct"() {
        def environmentVariables = [
            one: UUID.randomUUID().toString()
        ]
        def ext = Mock(JsonNode)
        def computeResources = Mock(ComputeResources)
        def images = [
            python: new Image.Builder().withName(UUID.randomUUID().toString()).build(),
            spark : new Image.Builder().withName(UUID.randomUUID().toString()).build()
        ]

        def builder = new JobEnvironment.Builder()

        when:
        def environment = builder.build()

        then:
        environment.getComputeResources() != null
        environment.getImages() != null
        environment.getEnvironmentVariables().isEmpty()
        !environment.getExt().isPresent()

        when:
        environment = builder
            .withComputeResources(computeResources)
            .withImages(images)
            .withEnvironmentVariables(environmentVariables)
            .withExt(ext)
            .build()

        then:
        environment.getComputeResources() == computeResources
        environment.getImages() == images
        environment.getEnvironmentVariables() == environmentVariables
        environment.getExt().orElse(null) == ext

        when:
        def environment2 = builder.build()
        def environment3 = builder
            .withImages([python: new Image.Builder().withName(UUID.randomUUID().toString()).build()])
            .build()

        then:
        environment == environment2
        environment != environment3
        environment.hashCode() == environment2.hashCode()
        environment.hashCode() != environment3.hashCode()
        environment.toString() == environment2.toString()
        environment.toString() != environment3.toString()
    }
}
