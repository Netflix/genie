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

package com.netflix.genie.web.services.impl

import com.netflix.genie.common.internal.dto.JobDirectoryManifest
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.services.AgentFileManifestService
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest)
class AgentFileManifestServiceImplSpec extends Specification {
    AgentFileManifestService service

    void setup() {
        this.service = new AgentFileManifestServiceImpl()
    }

    def "UpdateManifest"() {
        setup:
        String job1 = "job1"
        JobDirectoryManifest manifest1 = Mock(JobDirectoryManifest)
        String job2 = "job2"
        JobDirectoryManifest manifest2 = Mock(JobDirectoryManifest)

        Optional<JobDirectoryManifest> manifest

        when:
        service.updateManifest(job1, manifest1)
        manifest = service.getManifest(job1)

        then:
        manifest.get() == manifest1

        when:
        service.updateManifest(job1, manifest1)
        manifest = service.getManifest(job1)

        then:
        manifest.get() == manifest1

        when:
        service.updateManifest(job2, manifest2)
        manifest = service.getManifest(job2)

        then:
        manifest.get() == manifest2

        when:
        manifest = service.getManifest("...")

        then:
        !manifest.isPresent()

        when:
        service.updateManifest(job1, Mock(JobDirectoryManifest))
        manifest = service.getManifest(job1)

        then:
        manifest.get() != manifest1

        when:
        service.deleteManifest(job1)
        manifest = service.getManifest(job1)

        then:
        !manifest.isPresent()
    }
}
