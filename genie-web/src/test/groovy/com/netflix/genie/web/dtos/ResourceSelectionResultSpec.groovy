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
package com.netflix.genie.web.dtos


import com.netflix.genie.common.external.dtos.v4.Command
import spock.lang.Specification

/**
 * Specifications for {@link ResourceSelectionResult}.
 *
 * @author tgianos
 */
class ResourceSelectionResultSpec extends Specification {

    def "can build"() {
        ResourceSelectionResult<Command> commandSelectionResult
        Command command = Mock(Command)
        def selectionRationale = UUID.randomUUID().toString()

        when:
        commandSelectionResult = new ResourceSelectionResult.Builder<Command>(this.getClass()).build()

        then:
        commandSelectionResult.getSelectorClass() == this.getClass()
        !commandSelectionResult.getSelectedResource().isPresent()
        !commandSelectionResult.getSelectionRationale().isPresent()

        when:
        commandSelectionResult = new ResourceSelectionResult.Builder<Command>(this.getClass())
            .withSelectedResource(command)
            .withSelectionRationale(selectionRationale)
            .build()

        then:
        commandSelectionResult.getSelectorClass() == this.getClass()
        commandSelectionResult.getSelectedResource().orElse(null) == command
        commandSelectionResult.getSelectionRationale().orElse(null) == selectionRationale
    }
}
