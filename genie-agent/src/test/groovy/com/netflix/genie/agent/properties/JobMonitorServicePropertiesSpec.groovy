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
package com.netflix.genie.agent.properties

import org.springframework.util.unit.DataSize
import spock.lang.Specification

import java.time.Duration

class JobMonitorServicePropertiesSpec extends Specification {

    def "Defaults, getters, setters"() {
        when:
        JobMonitorServiceProperties p = new JobMonitorServiceProperties()

        then:
        p.getCheckInterval() == Duration.ofMinutes(1)
        p.getMaxFiles() == 64_000
        p.getMaxTotalSize() == DataSize.ofGigabytes(16)
        p.getMaxFileSize() == DataSize.ofGigabytes(8)
        p.getCheckRemoteJobStatus()

        when:
        p.setCheckInterval(Duration.ofSeconds(30))
        p.setMaxFiles(32_000)
        p.setMaxTotalSize(DataSize.ofGigabytes(8))
        p.setMaxFileSize(DataSize.ofGigabytes(4))
        p.setCheckRemoteJobStatus(false)

        then:
        p.getCheckInterval() == Duration.ofSeconds(30)
        p.getMaxFiles() == 32_000
        p.getMaxTotalSize() == DataSize.ofGigabytes(8)
        p.getMaxFileSize() == DataSize.ofGigabytes(4)
        !p.getCheckRemoteJobStatus()
    }
}
