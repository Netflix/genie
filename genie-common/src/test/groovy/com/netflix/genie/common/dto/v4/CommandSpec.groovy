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
package com.netflix.genie.common.dto.v4

import com.google.common.collect.Lists
import com.netflix.genie.common.dto.CommandStatus
import spock.lang.Specification

import java.time.Instant

/**
 * Specifications for the {@link Application} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
class CommandSpec extends Specification {

    def "Can build immutable command resource"() {
        def metadata = new CommandMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.ACTIVE
        ).build()
        def id = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        def created = Instant.now()
        def updated = Instant.now()
        def executable = Lists.newArrayList(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        def memory = 280
        def checkDelay = 389_132L
        Command command

        when:
        command = new Command(
                id,
                created,
                updated,
                resources,
                metadata,
                executable,
                memory,
                checkDelay
        )

        then:
        command.getId() == id
        command.getCreated() == created
        command.getUpdated() == updated
        command.getResources() == resources
        command.getMetadata() == metadata
        command.getExecutable() == executable
        command.getMemory().orElse(-1) == memory
        command.getCheckDelay() == checkDelay

        when:
        command = new Command(
                id,
                created,
                updated,
                null,
                metadata,
                executable,
                null,
                checkDelay
        )

        then:
        command.getId() == id
        command.getCreated() == created
        command.getUpdated() == updated
        command.getResources() == new ExecutionEnvironment(null, null, null)
        command.getMetadata() == metadata
        command.getExecutable() == executable
        !command.getMemory().isPresent()
        command.getCheckDelay() == checkDelay
    }
}
