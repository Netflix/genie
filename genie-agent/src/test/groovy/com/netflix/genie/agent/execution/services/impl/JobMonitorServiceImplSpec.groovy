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
package com.netflix.genie.agent.execution.services.impl

import com.netflix.genie.agent.execution.exceptions.GetJobStatusException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.services.JobMonitorService
import com.netflix.genie.agent.execution.services.KillService
import com.netflix.genie.agent.properties.AgentProperties
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.dtos.DirectoryManifest
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService
import org.springframework.scheduling.TaskScheduler
import org.springframework.util.unit.DataSize
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.ScheduledFuture

class JobMonitorServiceImplSpec extends Specification {

    KillService killService
    JobDirectoryManifestCreatorService manifestCreatorService
    AgentJobService agentJobService
    TaskScheduler taskScheduler
    AgentProperties agentProperties
    ScheduledFuture scheduledFuture
    JobMonitorService service
    String jobId

    void setup() {
        this.killService = Mock(KillService)
        this.manifestCreatorService = Mock(JobDirectoryManifestCreatorService)
        this.agentJobService = Mock(AgentJobService)
        this.taskScheduler = Mock(TaskScheduler)
        this.agentProperties = new AgentProperties()
        this.scheduledFuture = Mock(ScheduledFuture)
        this.jobId = UUID.randomUUID().toString()
        this.service = new JobMonitorServiceImpl(killService, manifestCreatorService, agentJobService, taskScheduler, agentProperties)
    }


    def "Do not kill"() {
        Path jobDirectoryPath = Mock(Path)
        DirectoryManifest directoryManifest = Mock(DirectoryManifest)
        Runnable task

        when:
        service.start(jobId, jobDirectoryPath)

        then:
        1 * taskScheduler.scheduleAtFixedRate(_ as Runnable, agentProperties.getJobMonitorService().getCheckInterval()) >> {
            Runnable r, Duration d ->
                task = r
                return scheduledFuture
        }
        task != null

        when:
        task.run()

        then:
        1 * manifestCreatorService.getDirectoryManifest(jobDirectoryPath) >> directoryManifest
        1 * directoryManifest.getNumFiles() >> 10
        1 * directoryManifest.getTotalSizeOfFiles() >> 1024
        1 * directoryManifest.getFiles() >> []
        1 * agentJobService.getJobStatus(jobId) >> JobStatus.RUNNING
        0 * killService.kill(KillService.KillSource.REMOTE_STATUS_MONITOR)

        when:
        service.stop()

        then:
        1 * scheduledFuture.cancel(true)
    }

    @Unroll
    def "Exceed #description limit"() {
        Path jobDirectoryPath = Mock(Path)
        DirectoryManifest directoryManifest = Mock(DirectoryManifest)
        Runnable task

        when:
        service.start(jobId, jobDirectoryPath)

        then:
        1 * taskScheduler.scheduleAtFixedRate(_ as Runnable, agentProperties.getJobMonitorService().getCheckInterval()) >> {
            Runnable r, Duration d ->
                task = r
                return scheduledFuture
        }
        task != null

        when:
        task.run()

        then:
        1 * manifestCreatorService.getDirectoryManifest(jobDirectoryPath) >> directoryManifest
        _ * directoryManifest.getNumFiles() >> numFiles
        _ * directoryManifest.getTotalSizeOfFiles() >> totalFilesSize
        _ * directoryManifest.getFiles() >> [
            Mock(DirectoryManifest.ManifestEntry) {
                getSize() >> 10
            },
            Mock(DirectoryManifest.ManifestEntry) {
                getSize() >> 0
            },
            Mock(DirectoryManifest.ManifestEntry) {
                getSize() >> largestFileSize
            },
            Mock(DirectoryManifest.ManifestEntry) {
                getSize() >> 1024
            }
        ]
        1 * killService.kill(KillService.KillSource.FILES_LIMIT)
        0 * this.agentJobService.getJobStatus(_)

        when:
        service.stop()

        then:
        1 * scheduledFuture.cancel(true)

        where:
        description         | numFiles  | totalFilesSize                      | largestFileSize
        "total files count" | 1_000_000 | DataSize.ofMegabytes(1).toBytes()   | DataSize.ofMegabytes(1).toBytes()
        "total files size"  | 1024      | DataSize.ofGigabytes(100).toBytes() | DataSize.ofMegabytes(1).toBytes()
        "large file"        | 1024      | DataSize.ofMegabytes(1).toBytes()   | DataSize.ofGigabytes(100).toBytes()
        "all"               | 1_000_000 | DataSize.ofGigabytes(100).toBytes() | DataSize.ofGigabytes(100).toBytes()
    }

    def "Job marked failed"() {
        Path jobDirectoryPath = Mock(Path)
        DirectoryManifest directoryManifest = Mock(DirectoryManifest)
        Runnable task

        when:
        service.start(jobId, jobDirectoryPath)

        then:
        1 * taskScheduler.scheduleAtFixedRate(_ as Runnable, agentProperties.getJobMonitorService().getCheckInterval()) >> {
            Runnable r, Duration d ->
                task = r
                return scheduledFuture
        }
        task != null

        when:
        task.run()

        then:
        1 * manifestCreatorService.getDirectoryManifest(jobDirectoryPath) >> directoryManifest
        1 * directoryManifest.getNumFiles() >> 10
        1 * directoryManifest.getTotalSizeOfFiles() >> 1024
        1 * directoryManifest.getFiles() >> []
        1 * agentJobService.getJobStatus(jobId) >> JobStatus.FAILED
        1 * killService.kill(KillService.KillSource.REMOTE_STATUS_MONITOR)

        when:
        service.stop()

        then:
        1 * scheduledFuture.cancel(true)
    }


    def "Job status check disabled failed"() {
        Path jobDirectoryPath = Mock(Path)
        DirectoryManifest directoryManifest = Mock(DirectoryManifest)
        Runnable task
        this.agentProperties.getJobMonitorService().setCheckRemoteJobStatus(false)

        when:
        service.start(jobId, jobDirectoryPath)

        then:
        1 * taskScheduler.scheduleAtFixedRate(_ as Runnable, agentProperties.getJobMonitorService().getCheckInterval()) >> {
            Runnable r, Duration d ->
                task = r
                return scheduledFuture
        }
        task != null

        when:
        task.run()

        then:
        1 * manifestCreatorService.getDirectoryManifest(jobDirectoryPath) >> directoryManifest
        1 * directoryManifest.getNumFiles() >> 10
        1 * directoryManifest.getTotalSizeOfFiles() >> 1024
        1 * directoryManifest.getFiles() >> []
        0 * agentJobService.getJobStatus(jobId)
        0 * killService.kill(KillService.KillSource.REMOTE_STATUS_MONITOR)

        when:
        service.stop()

        then:
        1 * scheduledFuture.cancel(true)
    }

    def "Handle service error"() {
        Path jobDirectoryPath = Mock(Path)
        Runnable task

        when:
        service.start(jobId, jobDirectoryPath)

        then:
        1 * taskScheduler.scheduleAtFixedRate(_ as Runnable, agentProperties.getJobMonitorService().getCheckInterval()) >> {
            Runnable r, Duration d ->
                task = r
                return scheduledFuture
        }
        task != null

        when:
        task.run()

        then:
        1 * manifestCreatorService.getDirectoryManifest(jobDirectoryPath) >> {
            throw new IOException("...")
        }
        1 * agentJobService.getJobStatus(jobId) >> {
            throw new GetJobStatusException("...")
        }
        0 * killService.kill(KillService.KillSource.REMOTE_STATUS_MONITOR)

        when:
        service.stop()

        then:
        1 * scheduledFuture.cancel(true)
    }
}
