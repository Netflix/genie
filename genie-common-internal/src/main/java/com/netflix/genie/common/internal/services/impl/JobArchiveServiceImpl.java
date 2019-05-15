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
package com.netflix.genie.common.internal.services.impl;

import com.google.common.collect.ImmutableList;
import com.netflix.genie.common.internal.dto.DirectoryManifest;
import com.netflix.genie.common.internal.exceptions.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.JobArchiver;
import com.netflix.genie.common.util.GenieObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Default implementation of the {@link JobArchiveService}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class JobArchiveServiceImpl implements JobArchiveService {

    private final ImmutableList<JobArchiver> jobArchivers;

    /**
     * Constructor.
     *
     * @param jobArchivers The ordered list of {@link JobArchiver} implementations to use. Not empty.
     */
    public JobArchiveServiceImpl(final List<JobArchiver> jobArchivers) {
        this.jobArchivers = ImmutableList.copyOf(jobArchivers);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void archiveDirectory(final Path directory, final URI target) throws JobArchiveException {
        // TODO: This relies highly on convention. Might be nicer to better abstract with database
        //       record that points directly to where the manifest is or other solution?
        try {
            final DirectoryManifest manifest = new DirectoryManifest(directory, true);
            final Path manifestDirectoryPath = StringUtils.isBlank(JobArchiveService.MANIFEST_DIRECTORY)
                ? directory
                : directory.resolve(JobArchiveService.MANIFEST_DIRECTORY);
            if (Files.notExists(manifestDirectoryPath)) {
                Files.createDirectories(manifestDirectoryPath);
            } else if (!Files.isDirectory(manifestDirectoryPath)) {
                throw new JobArchiveException(
                    manifestDirectoryPath + " is not a directory. Unable to create job manifest. Unable to archive"
                );
            }
            final Path manifestPath = manifestDirectoryPath.resolve(JobArchiveService.MANIFEST_NAME);
            Files.write(manifestPath, GenieObjectMapper.getMapper().writeValueAsBytes(manifest));
            log.debug("Wrote job directory manifest to {}", manifestPath);
        } catch (final IOException ioe) {
            throw new JobArchiveException("Unable to create job directory manifest. Unable to archive", ioe);
        }

        // Attempt to archive the job directory, now including the manifest file, using available implementations
        final String uriString = target.toString();
        for (final JobArchiver archiver : this.jobArchivers) {
            // TODO: Perhaps we should pass the manifest down to the archive implementations if they want to use it?
            if (archiver.archiveDirectory(directory, target)) {
                log.debug(
                    "Successfully archived job directory {} to {} using {}",
                    directory.toString(),
                    uriString,
                    archiver.getClass().getSimpleName()
                );
                return;
            }
        }

        // For now archival is not considered critical so just log warning
        log.warn(
            "Failed to archive job directory {} to {} using any of the available implementations",
            directory.toString(),
            uriString
        );
    }
}
