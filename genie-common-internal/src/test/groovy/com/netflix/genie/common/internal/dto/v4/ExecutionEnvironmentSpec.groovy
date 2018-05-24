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
package com.netflix.genie.common.internal.dto.v4

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

        when: "Empty resources are provided they're ignored"
        environment = new ExecutionEnvironment(
                Sets.newHashSet(" "),
                Sets.newHashSet("\t", "\n"),
                ""
        )

        then:
        !environment.getSetupFile().isPresent()
        environment.getConfigs().isEmpty()
        environment.getDependencies().isEmpty()
    }

    def "Test equals"() {
        def base = createExecutionEnvironment()
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
        comparable = createExecutionEnvironment()

        then:
        base != comparable

        when:
        comparable = "I'm definitely not the right type of object"

        then:
        base != comparable

        when:
        def config = UUID.randomUUID().toString()
        def dependency = UUID.randomUUID().toString()
        def setupFile = UUID.randomUUID().toString()
        base = new ExecutionEnvironment(Sets.newHashSet(config), Sets.newHashSet(dependency), setupFile)
        comparable = new ExecutionEnvironment(Sets.newHashSet(config), Sets.newHashSet(dependency), setupFile)

        then:
        base == comparable
    }

    def "Test hashCode"() {
        ExecutionEnvironment one
        ExecutionEnvironment two

        when:
        one = createExecutionEnvironment()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createExecutionEnvironment()
        two = createExecutionEnvironment()

        then:
        one.hashCode() != two.hashCode()

        when:
        def config = UUID.randomUUID().toString()
        def dependency = UUID.randomUUID().toString()
        def setupFile = UUID.randomUUID().toString()
        one = new ExecutionEnvironment(Sets.newHashSet(config), Sets.newHashSet(dependency), setupFile)
        two = new ExecutionEnvironment(Sets.newHashSet(config), Sets.newHashSet(dependency), setupFile)

        then:
        one.hashCode() == two.hashCode()
    }

    def "Test toString"() {
        ExecutionEnvironment one
        ExecutionEnvironment two

        when:
        one = createExecutionEnvironment()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createExecutionEnvironment()
        two = createExecutionEnvironment()

        then:
        one.toString() != two.toString()

        when:
        def config = UUID.randomUUID().toString()
        def dependency = UUID.randomUUID().toString()
        def setupFile = UUID.randomUUID().toString()
        one = new ExecutionEnvironment(Sets.newHashSet(config), Sets.newHashSet(dependency), setupFile)
        two = new ExecutionEnvironment(Sets.newHashSet(config), Sets.newHashSet(dependency), setupFile)

        then:
        one.toString() == two.toString()
    }

    ExecutionEnvironment createExecutionEnvironment() {
        return new ExecutionEnvironment(
                Sets.newHashSet(UUID.randomUUID().toString()),
                Sets.newHashSet(UUID.randomUUID().toString()),
                UUID.randomUUID().toString()
        )
    }
}
