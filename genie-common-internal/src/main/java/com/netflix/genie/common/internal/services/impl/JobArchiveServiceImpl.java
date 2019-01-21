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
import com.netflix.genie.common.internal.exceptions.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.JobArchiver;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
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
        final String uriString = target.toString();
        for (final JobArchiver archiver : this.jobArchivers) {
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

        log.warn(
            "Failed to archive job directory {} to {} using any of the available implementations",
            directory.toString(),
            uriString
        );
    }
}
