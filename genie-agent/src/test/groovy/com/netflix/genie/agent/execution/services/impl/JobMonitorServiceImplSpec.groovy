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

import com.netflix.genie.agent.execution.services.JobMonitorService
import com.netflix.genie.agent.execution.services.KillService
import com.netflix.genie.agent.properties.AgentProperties
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
    TaskScheduler taskScheduler
    AgentProperties agentProperties
    ScheduledFuture scheduledFuture
    JobMonitorService service

    void setup() {
        this.killService = Mock(KillService)
        this.manifestCreatorService = Mock(JobDirectoryManifestCreatorService)
        this.taskScheduler = Mock(TaskScheduler)
        this.agentProperties = new AgentProperties()
        this.scheduledFuture = Mock(ScheduledFuture)
        this.service = new JobMonitorServiceImpl(killService, manifestCreatorService, taskScheduler, agentProperties)
    }

    @Unroll
    def "Exceed #description limit"() {
        Path jobDirectoryPath = Mock(Path)
        DirectoryManifest directoryManifest = Mock(DirectoryManifest)
        Runnable task

        when:
        service.start(jobDirectoryPath)

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

        when:
        service.stop()

        then:
        1 * scheduledFuture.cancel(true)

        where:
        description         | numFiles  | totalFilesSize                      | largestFileSize
        "total files count" | 1_000_000 | DataSize.ofMegabytes(1).toBytes() | DataSize.ofMegabytes(1).toBytes()
        "total files size"  | 1024      | DataSize.ofGigabytes(100).toBytes() | DataSize.ofMegabytes(1).toBytes()
        "large file"        | 1024      | DataSize.ofMegabytes(1).toBytes()   | DataSize.ofGigabytes(100).toBytes()
    }
}
