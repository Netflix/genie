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

import com.netflix.genie.common.internal.exceptions.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.nio.file.CopyOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean archiveDirectory(final Path directory, final URI target) throws JobArchiveException {
        if (!target.getScheme().equalsIgnoreCase(FILE_SCHEME)) {
            return false;
        }

        final String sourceString = directory.toString();
        final String targetString = target.toString();

        try {
            log.debug("Attempting to archive job directory {} to {}", sourceString, targetString);
            Files.walkFileTree(directory, new JobDirectoryCopier(directory, Paths.get(target)));
            log.debug("Successfully archived job directory {} to {}", sourceString, targetString);
            return true;
        } catch (final IOException ioe) {
            throw new JobArchiveException("Unable to copy " + sourceString + " to " + targetString, ioe);
        }
    }

    @Slf4j
    private static class JobDirectoryCopier extends SimpleFileVisitor<Path> {

        private static final CopyOption[] COPY_OPTIONS = new CopyOption[]{
            StandardCopyOption.COPY_ATTRIBUTES,
            StandardCopyOption.REPLACE_EXISTING,
        };
        private final Path source;
        private final Path target;

        JobDirectoryCopier(final Path source, final Path target) {
            this.source = source;
            this.target = target;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            final Path newDirectory = this.target.resolve(this.source.relativize(dir));
            try {
                Files.copy(dir, newDirectory, COPY_OPTIONS);
            } catch (final IOException ioe) {
                log.error(
                    "Unable to create target directory {}. Skipping source directory {} subtree",
                    newDirectory,
                    dir,
                    ioe
                );
                return FileVisitResult.SKIP_SUBTREE;
            }
            return FileVisitResult.CONTINUE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            Files.copy(file, this.target.resolve(this.source.relativize(file)), COPY_OPTIONS);
            return FileVisitResult.CONTINUE;
        }
    }
}
