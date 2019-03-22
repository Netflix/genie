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
package com.netflix.genie.web.properties

import com.netflix.genie.test.suppliers.RandomSuppliers
import spock.lang.Specification

/**
 * Specifications for the {@link TasksExecutorPoolProperties} class.
 *
 * @author tgianos
 */
class TasksExecutorPoolPropertiesSpec extends Specification {

    def "Default parameters are as expected"() {
        when:
        def properties = new TasksExecutorPoolProperties()

        then:
        properties.getSize() == 2
        properties.getThreadNamePrefix() == "genie-task-executor-"
    }

    def "Can set new pool values"() {
        when:
        def properties = new TasksExecutorPoolProperties()

        then:
        properties.getSize() == 2
        properties.getThreadNamePrefix() == "genie-task-executor-"

        when:
        def newSize = RandomSuppliers.INT.get()
        properties.setSize(newSize)
        def newPrefix = RandomSuppliers.STRING.get()
        properties.setThreadNamePrefix(newPrefix)

        then:
        properties.getSize() == newSize
        properties.getThreadNamePrefix() == newPrefix
    }
}
