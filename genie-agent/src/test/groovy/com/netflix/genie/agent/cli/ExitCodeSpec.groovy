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

import com.netflix.genie.test.categories.UnitTest
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.util.Sets
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class ExitCodeSpec extends Specification {
    ExitCode[] exitCodes

    void setup() {
        this.exitCodes = ExitCode.values()
    }

    void cleanup() {
    }

    def "Distinct values"() {
        when:
        Set<Integer> codes = Sets.newHashSet();
        Set<String> messages = Sets.newHashSet();
        exitCodes.each {
            exitCode ->
                codes.add(exitCode.getCode())
                messages.add(exitCode.getMessage())
        }

        then:
        codes.size() == exitCodes.size()
        messages.size() == exitCodes.size()
        !messages.any { message -> StringUtils.isBlank(message) }
    }
}
