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

import com.google.common.collect.Sets
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the {@link ExecutionEnvironment} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class ExecutionEnvironmentSpec extends Specification {

    def "Can create immutable ExecutionEnvironment instance"() {
        def configs = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def dependencies = Sets.newHashSet(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
        def setupFile = UUID.randomUUID().toString()
        ExecutionEnvironment environment

        when:
        environment = new ExecutionEnvironment(configs, dependencies, setupFile)

        then:
        environment.getSetupFile().orElse(UUID.randomUUID().toString()) == setupFile
        environment.getConfigs() == configs
        environment.getDependencies() == dependencies

        when:
        environment = new ExecutionEnvironment(null, null, null)

        then:
        !environment.getSetupFile().isPresent()
        environment.getConfigs().isEmpty()
        environment.getDependencies().isEmpty()


        when:
        environment = new ExecutionEnvironment(configs, dependencies, setupFile)
        configs.add(UUID.randomUUID().toString())
        dependencies.add(UUID.randomUUID().toString())

        then:
        environment.getSetupFile().orElse(UUID.randomUUID().toString()) == setupFile
        environment.getConfigs() != configs
        environment.getDependencies() != dependencies
    }
}
