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
package com.netflix.genie.agent.execution.services.impl;

import com.netflix.genie.agent.execution.services.JobMonitorService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.properties.AgentProperties;
import com.netflix.genie.agent.properties.JobMonitorServiceProperties;
import com.netflix.genie.common.internal.dtos.DirectoryManifest;
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.unit.DataSize;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;

/**
 * Implementation of {@link JobMonitorService} that periodically checks on the size and number of files
 * using the manifest creator, rather than looking at the actual files.
 * This implementation is not thread safe.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class JobMonitorServiceImpl implements JobMonitorService {
    private final KillService killService;
    private final JobDirectoryManifestCreatorService manifestCreatorService;
    private final TaskScheduler taskScheduler;
    private final JobMonitorServiceProperties properties;
    private ScheduledFuture<?> scheduledCheck;

    JobMonitorServiceImpl(
        final KillService killService,
        final JobDirectoryManifestCreatorService manifestCreatorService,
        final TaskScheduler taskScheduler,
        final AgentProperties agentProperties) {
        this.killService = killService;
        this.manifestCreatorService = manifestCreatorService;
        this.taskScheduler = taskScheduler;
        this.properties = agentProperties.getJobMonitorService();
    }

    @Override
    public void start(final Path jobDirectory) {
        this.scheduledCheck = this.taskScheduler.scheduleAtFixedRate(
            () -> this.checkFilesSize(jobDirectory),
            this.properties.getCheckInterval()
        );

    }

    @Override
    public void stop() {
        if (this.scheduledCheck != null) {
            this.scheduledCheck.cancel(true);
        }
    }

    private void checkFilesSize(final Path jobDirectory) {
        final DirectoryManifest manifest;
        try {
            manifest = this.manifestCreatorService.getDirectoryManifest(jobDirectory);
        } catch (IOException e) {
            log.warn("Failed to obtain manifest: {}" + e.getMessage());
            return;
        }

        final int files = manifest.getNumFiles();
        final int maxFiles = this.properties.getMaxFiles();
        if (files > maxFiles) {
            log.error("Limit exceeded, too many files: {}/{}", files, maxFiles);
            this.killService.kill(KillService.KillSource.FILES_LIMIT);
            return;
        }

        final DataSize totalSize = DataSize.ofBytes(manifest.getTotalSizeOfFiles());
        final DataSize maxTotalSize = this.properties.getMaxTotalSize();
        if (totalSize.toBytes() > maxTotalSize.toBytes()) {
            log.error("Limit exceeded, job directory too large: {}/{}", totalSize, maxTotalSize);
            this.killService.kill(KillService.KillSource.FILES_LIMIT);
            return;
        }

        final Optional<DirectoryManifest.ManifestEntry> largestFile = manifest.getFiles()
            .stream()
            .max(Comparator.comparing(DirectoryManifest.ManifestEntry::getSize));

        if (largestFile.isPresent()) {
            final DataSize largestFileSize = DataSize.ofBytes(largestFile.get().getSize());
            final DataSize maxFileSize = this.properties.getMaxFileSize();
            if (largestFileSize.toBytes() > maxFileSize.toBytes()) {
                log.error(
                    "Limit exceeded, file too large: {}/{} ({})",
                    largestFileSize,
                    maxFileSize,
                    largestFile.get().getPath()
                );
                this.killService.kill(KillService.KillSource.FILES_LIMIT);
                return;
            }
        }

        log.debug("No files limit exceeded");
    }
}
