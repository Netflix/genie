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
import com.netflix.genie.agent.execution.process.JobProcessManager
import com.netflix.genie.agent.execution.process.JobProcessResult
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.services.JobSetupService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.dto.JobStatusMessages
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.internal.exceptions.JobArchiveException
import com.netflix.genie.common.internal.services.JobArchiveService
import spock.lang.Specification
import spock.lang.Unroll

class MonitorJobActionSpec extends Specification {
    String id
    ExecutionContext executionContext
    MonitorJobAction action
    Process process
    AgentJobService agentJobService
    JobProcessManager jobProcessManager
    JobSetupService jobSetupService
    JobArchiveService jobArchiveService
    ArgumentDelegates.CleanupArguments cleanupArguments
    File jobDirectory
    JobSpecification jobSpec
    CleanupStrategy cleanupStrategy
    String archiveLocation

    void setup() {
        this.id = UUID.randomUUID().toString()
        this.executionContext = Mock(ExecutionContext)
        this.agentJobService = Mock(AgentJobService)
        this.jobProcessManager = Mock(JobProcessManager)
        this.jobSetupService = Mock(JobSetupService)
        this.jobArchiveService = Mock(JobArchiveService)
        this.process = Mock(Process)
        this.cleanupArguments = Mock(ArgumentDelegates.CleanupArguments)
        this.jobDirectory = new File("/tmp/genie/jobs/XYZ")
        this.jobSpec = Mock(JobSpecification)
        this.cleanupStrategy = CleanupStrategy.DEPENDENCIES_CLEANUP
        this.archiveLocation = "s3://my-bucket/genie/jobs/XYZ"
        this.action = new MonitorJobAction(
            executionContext,
            agentJobService,
            jobProcessManager,
            jobSetupService,
            jobArchiveService,
            cleanupArguments
        )
    }

    @Unroll
    def "Successful #expectedJobStatus"() {
        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * jobProcessManager.waitFor() >> new JobProcessResult.Builder(finalStatus, finalStatusMessage).build()
        1 * executionContext.getClaimedJobId() >> Optional.of(id)
        1 * executionContext.getJobDirectory() >> Optional.of(jobDirectory)
        1 * executionContext.getJobSpecification() >> Optional.of(jobSpec)
        1 * cleanupArguments.getCleanupStrategy() >> cleanupStrategy
        1 * jobSetupService.cleanupJobDirectory(jobDirectory.toPath(), cleanupStrategy)
        1 * jobSpec.getArchiveLocation() >> Optional.of(archiveLocation)
        1 * jobArchiveService.archiveDirectory(jobDirectory.toPath(), new URI(archiveLocation))
        1 * agentJobService.changeJobStatus(id, JobStatus.RUNNING, finalStatus, finalStatusMessage)
        1 * executionContext.setCurrentJobStatus(finalStatus)
        1 * executionContext.setFinalJobStatus(finalStatus)

        expect:
        event == Events.MONITOR_JOB_COMPLETE

        where:
        finalStatus         | finalStatusMessage
        JobStatus.SUCCEEDED | JobStatusMessages.JOB_FINISHED_SUCCESSFULLY
        JobStatus.FAILED    | JobStatusMessages.JOB_FAILED
        JobStatus.KILLED    | JobStatusMessages.JOB_KILLED_BY_USER
    }

    def "Cleanup and archival errors"() {
        setup:
        def finalJobStatus = JobStatus.SUCCEEDED
        def finalJobStatusMessage = JobStatusMessages.JOB_FINISHED_SUCCESSFULLY

        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * jobProcessManager.waitFor() >> new JobProcessResult.Builder(finalJobStatus, finalJobStatusMessage).build()
        1 * executionContext.getClaimedJobId() >> Optional.of(id)
        1 * executionContext.getJobDirectory() >> Optional.of(jobDirectory)
        1 * executionContext.getJobSpecification() >> Optional.of(jobSpec)
        1 * cleanupArguments.getCleanupStrategy() >> cleanupStrategy
        1 * jobSetupService.cleanupJobDirectory(jobDirectory.toPath(), cleanupStrategy) >> {
            throw new IOException("...")
        }
        1 * jobSpec.getArchiveLocation() >> Optional.of(archiveLocation)
        1 * jobArchiveService.archiveDirectory(jobDirectory.toPath(), new URI(archiveLocation)) >> {
            throw new JobArchiveException("...")
        }
        1 * agentJobService.changeJobStatus(id, JobStatus.RUNNING, finalJobStatus, finalJobStatusMessage)
        1 * executionContext.setCurrentJobStatus(finalJobStatus)
        1 * executionContext.setFinalJobStatus(finalJobStatus)

        expect:
        event == Events.MONITOR_JOB_COMPLETE
    }

    def "Interrupt while monitoring"() {
        setup:
        def exception = new InterruptedException("...")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * jobProcessManager.waitFor() >> { throw exception }
        Exception e = thrown(RuntimeException)
        e.getCause() == exception
    }

    def "Change job status exception"() {
        JobStatus expectedJobStatus = JobStatus.SUCCEEDED
        String expectedJobStatusMessage = JobStatusMessages.JOB_FINISHED_SUCCESSFULLY
        JobProcessResult result = new JobProcessResult.Builder(expectedJobStatus, expectedJobStatusMessage).build()
        Exception exception = new ChangeJobStatusException("...")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * jobProcessManager.waitFor() >> result
        1 * executionContext.getClaimedJobId() >> Optional.of(id)
        1 * executionContext.getJobDirectory() >> Optional.of(jobDirectory)
        1 * executionContext.getJobSpecification() >> Optional.of(jobSpec)
        1 * cleanupArguments.getCleanupStrategy() >> cleanupStrategy
        1 * jobSetupService.cleanupJobDirectory(jobDirectory.toPath(), cleanupStrategy)
        1 * jobSpec.getArchiveLocation() >> Optional.empty()
        0 * jobArchiveService.archiveDirectory(_, _)
        1 * agentJobService.changeJobStatus(id, JobStatus.RUNNING, expectedJobStatus, expectedJobStatusMessage) >> {
            throw exception
        }
        Exception e = thrown(RuntimeException)
        e.getCause() == exception
    }

    def "Pre and post action validation"() {
        when:
        action.executePreActionValidation()

        then:
        1 * executionContext.getClaimedJobId() >> Optional.of(id)
        1 * executionContext.getCurrentJobStatus() >> Optional.of(JobStatus.RUNNING)
        1 * executionContext.getFinalJobStatus() >> Optional.empty()
        1 * executionContext.getJobSpecification() >> Optional.of(jobSpec)
        1 * executionContext.getJobDirectory() >> Optional.of(jobDirectory)

        when:
        action.executePostActionValidation()

        then:
        1 * executionContext.getCurrentJobStatus() >> Optional.of(JobStatus.KILLED)
        2 * executionContext.getFinalJobStatus() >> Optional.of(JobStatus.KILLED)
    }
}
