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

import com.netflix.genie.agent.execution.CleanupStrategy
import spock.lang.Specification

class CleanupArgumentsImplSpec extends Specification {

    def "GetCleanupStrategy"() {
        setup:
        CleanupArgumentsImpl cleanupArguments = new CleanupArgumentsImpl()

        expect:
        cleanupArguments.getCleanupStrategy() == CleanupStrategy.DEPENDENCIES_CLEANUP

        when:
        cleanupArguments.fullCleanup = false
        cleanupArguments.skipCleanup = false

        then:
        cleanupArguments.getCleanupStrategy() == CleanupStrategy.DEPENDENCIES_CLEANUP

        when:
        cleanupArguments.fullCleanup = true
        cleanupArguments.skipCleanup = false

        then:
        cleanupArguments.getCleanupStrategy() == CleanupStrategy.FULL_CLEANUP

        when:
        cleanupArguments.fullCleanup = false
        cleanupArguments.skipCleanup = true

        then:
        cleanupArguments.getCleanupStrategy() == CleanupStrategy.NO_CLEANUP
    }
}
