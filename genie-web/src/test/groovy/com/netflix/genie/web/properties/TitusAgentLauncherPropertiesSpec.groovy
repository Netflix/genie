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
package com.netflix.genie.web.properties

import org.springframework.util.unit.DataSize
import spock.lang.Specification

import java.time.Duration

class TitusAgentLauncherPropertiesSpec extends Specification {

    def "Defaults, getter, setter"() {

        TitusAgentLauncherProperties p = new TitusAgentLauncherProperties()

        expect:
        !p.isEnabled()
        p.getEndpoint() == URI.create("https://example-titus-endpoint.tld:1234")
        p.getEntryPointTemplate() == ["/bin/genie-agent"]
        p.getOwnerEmail() == "owners@genie.tld"
        p.getApplicationName() == "genie"
        p.getCapacityGroup() == "default"
        p.getSecurityAttributes() == [:]
        p.getSecurityGroups() == []
        p.getIAmRole() == "arn:aws:iam::000000000:role/SomeProfile"
        p.getImageName() == "image-name"
        p.getImageTag() == "latest"
        p.getRetries() == 0
        p.getRuntimeLimit() == Duration.ofHours(12)
        p.getGenieServerHost() == "example.genie.tld"
        p.getGenieServerPort() == 9090
        p.getHealthIndicatorMaxSize() == 100
        p.getHealthIndicatorExpiration() == Duration.ofMinutes(30)
        p.getAdditionalEnvironment() == [:]
        p.getAdditionalMemory().toGigabytes() == 2
        p.getAdditionalBandwidth().toMegabytes() == 0
        p.getAdditionalCPU() == 1
        p.getAdditionalDiskSize().toGigabytes() == 1
        p.getAdditionalGPU() == 0
        p.getMinimumBandwidth().toMegabytes() == 7
        p.getMinimumCPU() == 1
        p.getMinimumDiskSize().toGigabytes() == 10
        p.getMinimumMemory().toGigabytes() == 4
        p.getMinimumGPU() == 0
        p.getContainerAttributes() == [:]
        p.getAdditionalJobAttributes() == [:]
        p.getStack() == ""
        p.getDetail() == ""
        p.getSequence() == ""
        p.getCommandTemplate() == [
            "exec",
            "--api-job",
            "--launchInJobDirectory",
            "--job-id", TitusAgentLauncherProperties.JOB_ID_PLACEHOLDER,
            "--server-host", TitusAgentLauncherProperties.SERVER_HOST_PLACEHOLDER,
            "--server-port", TitusAgentLauncherProperties.SERVER_PORT_PLACEHOLDER
        ]

        when:
        p.setEnabled(true)
        p.setEndpoint(URI.create("https://test-titus-endpoint.tld:4321"))
        p.setEntryPointTemplate(
            [
                "/usr/local/bin/genie-agent",
                "exec"
            ]
        )
        p.setOwnerEmail("genie@genie.tld")
        p.setApplicationName("genie-foo")
        p.setCapacityGroup("genie-cg")
        p.setSecurityAttributes([foo: "bar"])
        p.setSecurityGroups(["g1", "g2"])
        p.setIAmRole("arn:aws:iam::99999999:role/SomeOtherProfile")
        p.setImageName("another-image-name")
        p.setImageTag("latest.release")
        p.setRetries(1)
        p.setRuntimeLimit(Duration.ofHours(24))
        p.setGenieServerHost("genie.tld")
        p.setGenieServerPort(9191)
        p.setHealthIndicatorMaxSize(200)
        p.setHealthIndicatorExpiration(Duration.ofMinutes(15))
        p.setAdditionalEnvironment([FOO: "BAR"])
        p.setAdditionalMemory(DataSize.ofGigabytes(4))
        p.setAdditionalBandwidth(DataSize.ofMegabytes(2))
        p.setAdditionalCPU(2)
        p.setAdditionalDiskSize(DataSize.ofGigabytes(2))
        p.setAdditionalGPU(1)
        p.setMinimumBandwidth(DataSize.ofMegabytes(14))
        p.setMinimumCPU(2)
        p.setMinimumDiskSize(DataSize.ofGigabytes(20))
        p.setMinimumMemory(DataSize.ofGigabytes(8))
        p.setMinimumGPU(1)
        p.setContainerAttributes(["hi": "bye"])
        p.setAdditionalJobAttributes(["new": "attribute"])
        p.setStack("stack")
        p.setDetail("detail")
        p.setSequence("sequence")
        p.setCommandTemplate(
            [
                "--server-host", TitusAgentLauncherProperties.SERVER_HOST_PLACEHOLDER,
                "--server-port", TitusAgentLauncherProperties.SERVER_PORT_PLACEHOLDER,
                "--job-id", TitusAgentLauncherProperties.JOB_ID_PLACEHOLDER,
                "--api-job",
                "--launchInJobDirectory"
            ]
        )

        then:
        p.isEnabled()
        p.getEndpoint() == URI.create("https://test-titus-endpoint.tld:4321")
        p.getEntryPointTemplate() == [
            "/usr/local/bin/genie-agent",
            "exec"
        ]
        p.getOwnerEmail() == "genie@genie.tld"
        p.getApplicationName() == "genie-foo"
        p.getCapacityGroup() == "genie-cg"
        p.getSecurityAttributes() == [foo: "bar"]
        p.getSecurityGroups() == ["g1", "g2"]
        p.getIAmRole() == "arn:aws:iam::99999999:role/SomeOtherProfile"
        p.getImageName() == "another-image-name"
        p.getImageTag() == "latest.release"
        p.getRetries() == 1
        p.getRuntimeLimit() == Duration.ofHours(24)
        p.getGenieServerHost() == "genie.tld"
        p.getGenieServerPort() == 9191
        p.getHealthIndicatorMaxSize() == 200
        p.getHealthIndicatorExpiration() == Duration.ofMinutes(15)
        p.getAdditionalEnvironment() == [FOO: "BAR"]
        p.getAdditionalMemory() == DataSize.ofGigabytes(4)
        p.getAdditionalMemory().toGigabytes() == 4
        p.getAdditionalBandwidth().toMegabytes() == 2
        p.getAdditionalCPU() == 2
        p.getAdditionalGPU() == 1
        p.getAdditionalDiskSize().toGigabytes() == 2
        p.getMinimumBandwidth().toMegabytes() == 14
        p.getMinimumCPU() == 2
        p.getMinimumDiskSize().toGigabytes() == 20
        p.getMinimumMemory().toGigabytes() == 8
        p.getMinimumGPU() == 1
        p.getContainerAttributes() == ["hi": "bye"]
        p.getAdditionalJobAttributes() == ["new": "attribute"]
        p.getStack() == "stack"
        p.getDetail() == "detail"
        p.getSequence() == "sequence"
        p.getCommandTemplate() == [
            "--server-host", TitusAgentLauncherProperties.SERVER_HOST_PLACEHOLDER,
            "--server-port", TitusAgentLauncherProperties.SERVER_PORT_PLACEHOLDER,
            "--job-id", TitusAgentLauncherProperties.JOB_ID_PLACEHOLDER,
            "--api-job",
            "--launchInJobDirectory"
        ]
    }
}
