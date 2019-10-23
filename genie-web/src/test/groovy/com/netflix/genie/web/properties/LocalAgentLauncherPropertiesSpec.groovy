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

import com.google.common.collect.ImmutableMap
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
        properties.getLaunchCommandTemplate() == ["java", "-jar", LocalAgentLauncherProperties.AGENT_JAR_PLACEHOLDER, "exec", "--serverHost", "127.0.0.1", "--serverPort", LocalAgentLauncherProperties.SERVER_PORT_PLACEHOLDER, "--api-job", "--jobId", LocalAgentLauncherProperties.JOB_ID_PLACEHOLDER]
        properties.getAgentJarPath() == "/tmp/genie-agent.jar"
        properties.getMaxJobMemory() == 10_240
        properties.getMaxTotalJobMemory() == 30_720L
        !properties.isRunAsUserEnabled()
        properties.additionalEnvironment.isEmpty()
        !properties.isProcessOutputCaptureEnabled()
    }

    def "Setters and getters work properly"() {
        def newExecutable = Lists.newArrayList("/etc/genie-agent.sh", "run")
        def newJar = "/tmp/agent-SNAPSHOT.jar"
        def newMaxJobMemory = 30_000
        def newMaxTotalJobMemory = 900_000L
        def properties = new LocalAgentLauncherProperties()
        def environment = ImmutableMap.of("FOO", "Bar")

        when:
        properties.setLaunchCommandTemplate(newExecutable)
        properties.setAgentJarPath(newJar)
        properties.setMaxJobMemory(newMaxJobMemory)
        properties.setMaxTotalJobMemory(newMaxTotalJobMemory)
        properties.setRunAsUserEnabled(true)
        properties.setAdditionalEnvironment(environment)
        properties.setProcessOutputCaptureEnabled(true)

        then:
        properties.getLaunchCommandTemplate() == newExecutable
        properties.getAgentJarPath() == newJar
        properties.getMaxJobMemory() == newMaxJobMemory
        properties.getMaxTotalJobMemory() == newMaxTotalJobMemory
        properties.isRunAsUserEnabled()
        properties.getAdditionalEnvironment() == environment
        properties.isProcessOutputCaptureEnabled()
    }
}
