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

import com.netflix.genie.common.internal.exceptions.checked.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiver;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * An implementation of {@link JobArchiver} which attempts to copy the job directory somewhere else on the file
 * system for backup. A convenient example of this would be a NFS mounted to the Genie host.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class FileSystemJobArchiverImpl implements JobArchiver {

    private static final String FILE_SCHEME = "file";
    private static final CopyOption[] COPY_OPTIONS = new CopyOption[]{
        StandardCopyOption.COPY_ATTRIBUTES,
        StandardCopyOption.REPLACE_EXISTING,
    };

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean archiveDirectory(
        final Path directory,
        final List<File> filesList,
        final URI target
    ) throws JobArchiveException {
        if (!target.getScheme().equalsIgnoreCase(FILE_SCHEME)) {
            return false;
        }

        final Path targetDirectoryPath = Paths.get(target.getPath());

        // If the source doesn't exist or isn't a directory, then throw an exception
        if (!Files.exists(directory) || !Files.isDirectory(directory)) {
            throw new JobArchiveException(directory + " doesn't exist or isn't a directory. Unable to copy");
        }

        // If the destination base directory exists and isn't a directory, then throw an exception
        if (Files.exists(targetDirectoryPath) && !Files.isDirectory(targetDirectoryPath)) {
            throw new JobArchiveException(targetDirectoryPath + " exist and isn't a directory. Unable to copy");
        }

        for (final File file : filesList) {
            final Path sourceFilePath = file.toPath();
            final Path sourceFileRelativePath = directory.relativize(sourceFilePath);
            final Path destinationFilePath = targetDirectoryPath.resolve(sourceFileRelativePath);
            try {
                final Path parentDirectory = destinationFilePath.getParent();
                if (parentDirectory != null) {
                    log.info("Creating parent directory for {}", destinationFilePath);
                    Files.createDirectories(parentDirectory);
                }
                log.info("Copying {} to {}", sourceFilePath, destinationFilePath);
                Files.copy(sourceFilePath, destinationFilePath, COPY_OPTIONS);
            } catch (IOException e) {
                log.warn("Failed to archive file {} to {}: {}", sourceFilePath, destinationFilePath, e.getMessage(), e);
            }
        }

        return true;
    }
}
