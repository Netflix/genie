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

package com.netflix.genie.agent.cli

import spock.lang.Specification
import spock.lang.Unroll

import java.util.stream.Collectors

class CommandNamesSpec extends Specification {

    @Unroll
    def "CommandNames #fieldName" (String fieldName, String fieldValue) {
        expect:
        fieldName.compareToIgnoreCase(fieldValue) == 0

        where:
        [fieldName, fieldValue] << createFieldNameValuePairs()
    }

    List<List<String>> createFieldNameValuePairs() {
        return CommandNames.getCommandNamesFields()
            .stream()
            .map { field ->
                [field.getName(), (String) field.get(null)] as String[]
            }
            .collect(Collectors.toList())
    }
}
