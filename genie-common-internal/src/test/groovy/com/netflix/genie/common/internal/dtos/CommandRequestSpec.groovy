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
package com.netflix.genie.common.internal.dtos

import spock.lang.Specification

/**
 * Specifications for the {@link CommandRequest} class.
 *
 * @author tgianos
 */
class CommandRequestSpec extends Specification {

    def "Can build immutable command request"() {
        def metadata = new CommandMetadata.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            CommandStatus.ACTIVE
        ).build()
        def requestedId = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def executable = [UUID.randomUUID().toString(), UUID.randomUUID().toString()]
        def clusterCriteria = [
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withName(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withStatus(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withVersion(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withTags([UUID.randomUUID().toString()].toSet()).build()
        ]
        def computeResources = DtoSpecUtils.getRandomComputeResources()
        def images = [
            foo: DtoSpecUtils.getRandomImage(),
            bar: DtoSpecUtils.getRandomImage()
        ]
        CommandRequest request

        when:
        request = new CommandRequest.Builder(metadata, executable)
            .withRequestedId(requestedId)
            .withResources(resources)
            .withClusterCriteria(clusterCriteria)
            .withComputeResources(computeResources)
            .withImages(images)
            .build()

        then:
        request.getMetadata() == metadata
        request.getRequestedId().orElse(UUID.randomUUID().toString()) == requestedId
        request.getResources() == resources
        request.getExecutable() == executable
        request.getClusterCriteria() == clusterCriteria
        request.getComputeResources() == Optional.ofNullable(computeResources)
        request.getImages() == images

        when:
        request = new CommandRequest.Builder(metadata, executable).build()

        then:
        request.getMetadata() == metadata
        !request.getRequestedId().isPresent()
        request.getResources() != null
        request.getExecutable() == executable
        request.getClusterCriteria().isEmpty()
        !request.getComputeResources().isPresent()
        request.getImages().isEmpty()

        when: "Optional fields are blank they're ignored"
        def newExecutable = new ArrayList(executable)
        newExecutable.add("\t")
        newExecutable.add(" ")
        newExecutable.add("")
        request = new CommandRequest.Builder(metadata, newExecutable)
            .withRequestedId(" ")
            .withResources(resources)
            .withClusterCriteria(null)
            .withComputeResources(null)
            .withImages(null)
            .build()

        then:
        request.getMetadata() == metadata
        !request.getRequestedId().isPresent()
        request.getResources() == resources
        request.getExecutable() == executable
        request.getClusterCriteria().isEmpty()
        !request.getComputeResources().isPresent()
        request.getImages().isEmpty()
    }

    def "Test equals"() {
        def base = DtoSpecUtils.getRandomCommandRequest()
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
        comparable = new CommandRequest.Builder(Mock(CommandMetadata), [UUID.randomUUID().toString()])
            .withRequestedId(UUID.randomUUID().toString())
            .toString()

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
        def status = CommandStatus.INACTIVE
        def binary = UUID.randomUUID().toString()
        def baseMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        base = new CommandRequest.Builder(baseMetadata, [binary]).build()
        comparable = new CommandRequest.Builder(comparableMetadata, [binary]).build()

        then:
        base == comparable
    }

    def "Test hashCode"() {
        CommandRequest one
        CommandRequest two

        when:
        one = DtoSpecUtils.getRandomCommandRequest()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = DtoSpecUtils.getRandomCommandRequest()
        two = DtoSpecUtils.getRandomCommandRequest()

        then:
        one.hashCode() != two.hashCode()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = CommandStatus.INACTIVE
        def binary = UUID.randomUUID().toString()
        def baseMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        one = new CommandRequest.Builder(baseMetadata, [binary]).build()
        two = new CommandRequest.Builder(comparableMetadata, [binary]).build()

        then:
        one.hashCode() == two.hashCode()
    }

    def "toString is consistent"() {
        CommandRequest one
        CommandRequest two

        when:
        one = DtoSpecUtils.getRandomCommandRequest()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = DtoSpecUtils.getRandomCommandRequest()
        two = DtoSpecUtils.getRandomCommandRequest()

        then:
        one.toString() != two.toString()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = CommandStatus.INACTIVE
        def binary = UUID.randomUUID().toString()
        def baseMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new CommandMetadata.Builder(name, user, version, status).build()
        one = new CommandRequest.Builder(baseMetadata, [binary]).build()
        two = new CommandRequest.Builder(comparableMetadata, [binary]).build()

        then:
        one.toString() == two.toString()
    }
}
