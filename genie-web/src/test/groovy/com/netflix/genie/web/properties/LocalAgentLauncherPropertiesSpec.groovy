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
package com.netflix.genie.web.properties

import com.google.common.collect.Lists
import spock.lang.Specification

/**
 * Specifications for {@link LocalAgentLauncherProperties}.
 *
 * @author tgianos
 */
class LocalAgentLauncherPropertiesSpec extends Specification {

    def "the property prefix is correct"() {
        expect:
        LocalAgentLauncherProperties.PROPERTY_PREFIX == "genie.agent.launcher.local"
    }

    def "The default values are correct"() {
        when:
        def properties = new LocalAgentLauncherProperties()

        then:
        properties.getExecutable() == Lists.newArrayList("java", "-jar", "/tmp/genie-agent.jar")
    }

    def "Setters and getters work properly"() {
        def newExecutable = Lists.newArrayList("/etc/genie-agent.sh", "run")
        def properties = new LocalAgentLauncherProperties()

        when:
        properties.setExecutable(newExecutable)

        then:
        properties.getExecutable() == newExecutable
    }
}
