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

package com.netflix.genie.agent.execution.statemachine.actions

import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.services.ArchivalService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.test.categories.UnitTest
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

@Category(UnitTest.class)
class ShutdownActionSpec extends Specification {
    @Rule
    TemporaryFolder temporaryFolder
    ExecutionContext executionContext
    ArchivalService archivalService
    ShutdownAction action
    JobSpecification jobSpecification
    File jobDir
    URI sampleS3URI = new URI("s3://test-bucket/genie/job/test123")

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.archivalService = Mock(ArchivalService)
        this.jobSpecification = Mock(JobSpecification)
        this.jobDir = temporaryFolder.newFolder("job")
        this.action = new ShutdownAction(executionContext, archivalService)
    }

    void cleanup() {
    }

    def "Archival works on shutdown"() {
        def event

        when:
        event = action.executeStateAction(executionContext)

        then: "Archive for a valid s3 archive location"
        1 * executionContext.getJobSpecification() >> jobSpecification
        1 * jobSpecification.getArchiveLocation() >> Optional.of(sampleS3URI.toString())
        1 * executionContext.getJobDirectory() >> jobDir
        1 * archivalService.archive(jobDir.toPath(), sampleS3URI)
        event == Events.SHUTDOWN_COMPLETE

        when:
        event = action.executeStateAction(executionContext)

        then: "Skip archival for a null archive location"
        1 * executionContext.getJobSpecification() >> jobSpecification
        1 * jobSpecification.getArchiveLocation() >> Optional.empty()
        0 * executionContext.getJobDirectory() >> jobDir
        0 * archivalService.archive(jobDir, _ as URI)
        event == Events.SHUTDOWN_COMPLETE

        when:
        event = action.executeStateAction(executionContext)

        then: "Skip archival for a empty archive location"
        1 * executionContext.getJobSpecification() >> jobSpecification
        1 * jobSpecification.getArchiveLocation() >> Optional.of("")
        0 * executionContext.getJobDirectory() >> jobDir
        0 * archivalService.archive(jobDir, _ as URI)
        event == Events.SHUTDOWN_COMPLETE
    }
}
