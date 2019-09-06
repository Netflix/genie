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

import com.netflix.genie.agent.cli.ArgumentDelegates
import com.netflix.genie.agent.execution.CleanupStrategy
import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException
import com.netflix.genie.agent.execution.exceptions.DownloadException
import com.netflix.genie.agent.execution.exceptions.SetUpJobException
import com.netflix.genie.agent.execution.services.AgentFileStreamService
import com.netflix.genie.agent.execution.services.AgentHeartBeatService
import com.netflix.genie.agent.execution.services.AgentJobKillService
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.services.DownloadService
import com.netflix.genie.agent.execution.services.JobSetupService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.agent.utils.EnvUtils
import com.netflix.genie.agent.utils.PathUtils
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.internal.jobs.JobConstants
import org.assertj.core.util.Sets
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

class SetUpJobActionSpec extends Specification {

    ExecutionContext executionContext

    JobSetupService jobSetupService
    AgentJobService agentJobService
    AgentHeartBeatService heartbeatService
    AgentJobKillService killService
    AgentFileStreamService fileStreamService
    ArgumentDelegates.CleanupArguments cleanupArguments
    SetUpJobAction action

    String jobId
    JobSpecification spec
    JobSpecification.ExecutionResource job

    File jobDir
    List<File> setupFiles
    Map<String, String> envMap
    CleanupStrategy cleanupStrategy

    void setup() {

        this.jobId = UUID.randomUUID().toString()
        this.executionContext = Mock(ExecutionContext)
        this.spec = Mock(JobSpecification)
        this.job = Mock(JobSpecification.ExecutionResource)

        this.jobDir = Paths.get("/tmp/genie/jobs/" + jobId).toFile()
        this.setupFiles = []
        this.envMap = [:]
        this.cleanupStrategy = CleanupStrategy.FULL_CLEANUP

        this.agentJobService = Mock(AgentJobService)
        this.jobSetupService = Mock(JobSetupService)
        this.heartbeatService = Mock(AgentHeartBeatService)
        this.killService = Mock(AgentJobKillService)
        this.fileStreamService = Mock(AgentFileStreamService)
        this.cleanupArguments = Mock(ArgumentDelegates.CleanupArguments)

        this.action = new SetUpJobAction(executionContext, jobSetupService, agentJobService, heartbeatService, killService, fileStreamService, cleanupArguments)
    }

    void cleanup() {
    }

    def "Successful action execution and cleanup"() {
        setup:

        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> Optional.of(spec)
        1 * spec.getJob() >> job
        1 * job.getId() >> jobId
        1 * heartbeatService.start(jobId)
        1 * killService.start(jobId)
        1 * jobSetupService.createJobDirectory(spec) >> jobDir
        1 * executionContext.setJobDirectory(jobDir)
        1 * agentJobService.changeJobStatus(jobId, JobStatus.CLAIMED, JobStatus.INIT, _ as String)
        1 * executionContext.setCurrentJobStatus(JobStatus.INIT)
        1 * fileStreamService.start(jobId, jobDir.toPath())
        1 * jobSetupService.downloadJobResources(spec, jobDir) >> setupFiles
        1 * jobSetupService.setupJobEnvironment(jobDir, spec, setupFiles) >> envMap
        1 * executionContext.setJobEnvironment(envMap)
        event == Events.SETUP_JOB_COMPLETE

        when:
        action.executeStateActionCleanup(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> Optional.of(jobDir)
        1 * cleanupArguments.getCleanupStrategy() >> cleanupStrategy
        1 * jobSetupService.cleanupJobDirectory(jobDir.toPath(), cleanupStrategy)
        1 * killService.stop()
        1 * heartbeatService.stop()
        1 * fileStreamService.stop()
    }

    def "Exception handling"() {
        setup:
        Exception setupException = new SetUpJobException("...")
        Exception changeJobStatusException = new ChangeJobStatusException("...")
        Exception ioException = new IOException("...")
        Exception e

        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> Optional.of(spec)
        1 * spec.getJob() >> job
        1 * job.getId() >> jobId
        1 * heartbeatService.start(jobId)
        1 * killService.start(jobId)
        1 * jobSetupService.createJobDirectory(spec) >> {throw setupException}
        e = thrown(RuntimeException)
        e.getCause() == setupException

        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getJobSpecification() >> Optional.of(spec)
        1 * spec.getJob() >> job
        1 * job.getId() >> jobId
        1 * heartbeatService.start(jobId)
        1 * killService.start(jobId)
        1 * jobSetupService.createJobDirectory(spec) >> jobDir
        1 * executionContext.setJobDirectory(jobDir)
        1 * agentJobService.changeJobStatus(jobId, JobStatus.CLAIMED, JobStatus.INIT, _ as String) >> {throw changeJobStatusException}
        e = thrown(RuntimeException)
        e.getCause() == changeJobStatusException

        when:
        action.executeStateActionCleanup(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> Optional.of(jobDir)
        1 * cleanupArguments.getCleanupStrategy() >> cleanupStrategy
        1 * jobSetupService.cleanupJobDirectory(jobDir.toPath(), cleanupStrategy) >> { throw ioException}
        1 * killService.stop()
        1 * heartbeatService.stop()
        1 * fileStreamService.stop()

        when:
        action.executeStateActionCleanup(executionContext)

        then:
        1 * executionContext.getJobDirectory() >> Optional.empty()
        0 * cleanupArguments.getCleanupStrategy() >> cleanupStrategy
        0 * jobSetupService.cleanupJobDirectory(jobDir.toPath(), cleanupStrategy) >> { throw ioException}
        1 * killService.stop()
        1 * heartbeatService.stop()
        1 * fileStreamService.stop()
    }

    def "Pre and post action validation"() {
        when:
        action.executePreActionValidation()

        then:
        1 * executionContext.getClaimedJobId() >> Optional.of(jobId)
        1 * executionContext.getCurrentJobStatus() >> Optional.of(JobStatus.CLAIMED)
        1 * executionContext.getJobSpecification() >> Optional.of(spec)
        1 * executionContext.getJobDirectory() >> Optional.empty()
        1 * executionContext.getJobEnvironment() >> Optional.empty()

        when:
        action.executePostActionValidation()

        then:
        1 * executionContext.getCurrentJobStatus() >> Optional.of(JobStatus.INIT)
        1 * executionContext.getJobDirectory() >> Optional.of(jobDir)
        1 * executionContext.getJobEnvironment() >> Optional.of(envMap)
    }
}
