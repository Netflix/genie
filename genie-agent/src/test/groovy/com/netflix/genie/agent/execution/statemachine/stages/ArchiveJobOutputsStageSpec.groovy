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
package com.netflix.genie.agent.execution.statemachine.stages

import com.netflix.genie.agent.execution.exceptions.ChangeJobArchiveStatusException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.internal.exceptions.checked.JobArchiveException
import com.netflix.genie.common.internal.services.JobArchiveService
import spock.lang.Specification

import java.nio.file.Path

class ArchiveJobOutputsStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    JobArchiveService jobArchiveService
    AgentJobService agentJobService
    JobSpecification jobSpec
    File jobDir
    String archiveLocation
    Path jobDirPath
    String jobId

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.jobArchiveService = Mock(JobArchiveService)
        this.agentJobService = Mock(AgentJobService)
        this.jobSpec = Mock(JobSpecification)
        this.jobDir = Mock(File)
        this.archiveLocation = "s3://genie-logs/foo/bar"
        this.jobDirPath = Mock(Path)
        this.jobId = UUID.randomUUID().toString()
        this.stage = new ArchiveJobOutputsStage(jobArchiveService, agentJobService)
    }

    def "AttemptTransition - success"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobDir
        1 * executionContext.getClaimedJobId() >> jobId
        1 * jobSpec.getArchiveLocation() >> Optional.of(archiveLocation)
        1 * jobDir.toPath() >> jobDirPath
        1 * jobArchiveService.archiveDirectory(jobDirPath, _ as URI)
        1 * agentJobService.changeJobArchiveStatus(jobId, ArchiveStatus.ARCHIVED)
    }

    def "AttemptTransition - no spec"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> null
        1 * executionContext.getJobDirectory() >> null
    }

    def "AttemptTransition - no archive location"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobDir
        1 * jobSpec.getArchiveLocation() >> Optional.empty()
    }

    def "AttemptTransition - archive error"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobDir
        1 * executionContext.getClaimedJobId() >> jobId
        1 * jobSpec.getArchiveLocation() >> Optional.of(archiveLocation)
        1 * jobDir.toPath() >> jobDirPath
        1 * jobArchiveService.archiveDirectory(jobDirPath, _ as URI) >> { throw new JobArchiveException() }
        1 * agentJobService.changeJobArchiveStatus(jobId, ArchiveStatus.FAILED)
        noExceptionThrown()
    }

    def "AttemptTransition - status update error"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> jobSpec
        1 * executionContext.getJobDirectory() >> jobDir
        1 * executionContext.getClaimedJobId() >> jobId
        1 * jobSpec.getArchiveLocation() >> Optional.of(archiveLocation)
        1 * jobDir.toPath() >> jobDirPath
        1 * jobArchiveService.archiveDirectory(jobDirPath, _ as URI)
        1 * agentJobService.changeJobArchiveStatus(jobId, ArchiveStatus.ARCHIVED) >> { throw new ChangeJobArchiveStatusException("...") }
        noExceptionThrown()
    }

}
