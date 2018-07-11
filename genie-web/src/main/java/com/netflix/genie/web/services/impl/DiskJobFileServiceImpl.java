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
package com.netflix.genie.web.services.impl;

import com.google.common.collect.Sets;
import com.netflix.genie.common.internal.dto.v4.files.JobFileState;
import com.netflix.genie.web.services.JobFileService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.PathResource;
import org.springframework.core.io.Resource;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.Set;

/**
 * A local disk based implementation of the {@link JobFileService} interface.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class DiskJobFileServiceImpl implements JobFileService {

//    private static final int DEFAULT_BUFFER_SIZE = 1024;

    private final Path jobsDirRoot;

    /**
     * Constructor.
     *
     * @param jobsDir The job directory resource
     * @throws IOException When the job directory can't be created or isn't a directory
     */
    public DiskJobFileServiceImpl(final Resource jobsDir) throws IOException {
        /*
           TODO: Note there is a @Bean for jobs dir but it's currently returned as a Spring Resource interface
                 This abstracts the actual underlying implementation and we can't guarantee it's on disk in that case
                 as it could be on S3 or some other storage medium and the Resource masks those details (correctly).
                 We can revisit this going forward but we may want to centralize the logic here. For now just keeping
                 local reference to the path on disk of the root of the jobs directory as specified by the property.
         */

        /*
            TODO: Fix using jobDir as a resource reference at some point... but integration tests swap out the
            reference and don't rely on properties. Too much to fix right now with other priorities.
            This should probably be based off the property.
         */

        // TODO: This throws an InvalidPathException ... Should we catch or just propagate as this will be called
        //       at startup and the system won't even start
        this.jobsDirRoot = jobsDir.getFile().toPath();

        // Make sure the root exists and is a directory
        this.createOrCheckDirectory(this.jobsDirRoot);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobDirectory(final String jobId) throws IOException {
        // TODO: Throws InvalidPathException...catch and rethrow or let it go? Even Java SDK throws Marco nemesis
        final Path jobDirectory = this.jobsDirRoot.resolve(jobId);
        this.createOrCheckDirectory(jobDirectory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<JobFileState> getJobDirectoryFileState(
        final String jobId,
        final boolean calculateMd5
    ) throws IOException {
        log.debug("Getting job directory state for job {} {} MD5", jobId, calculateMd5 ? "with" : "without");
        // TODO: It's possible the system should lock this directory while reading for consistent state?
        final Path jobDirectory = this.jobsDirRoot.resolve(jobId);
        this.createOrCheckDirectory(jobDirectory);

        final Set<JobFileState> jobDirectoryFiles = Sets.newHashSet();
        Files.walkFileTree(jobDirectory, new FileStateVisitor(jobDirectory, calculateMd5, jobDirectoryFiles));

        return jobDirectoryFiles;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    // TODO: We should be careful about how large the byte[] is. Perhaps we should have precondition to protect memory
    //       or we should wrap calls to this in something that chunks it off an input stream or just take this in as
    //       input stream
    public void updateFile(
        final String jobId,
        final String relativePath,
        final long startByte,
        final byte[] data
    ) throws IOException {
        log.debug(
            "Attempting to write {} bytes from position {} into log file {} for job {}",
            data.length,
            startByte,
            relativePath,
            jobId
        );
        final Path jobFile = this.jobsDirRoot.resolve(jobId).resolve(relativePath);

        if (Files.notExists(jobFile)) {
            // Make sure all the directories exist on disk
            final Path logFileParent = jobFile.getParent();
            if (logFileParent != null) {
                this.createOrCheckDirectory(logFileParent);
            }
        } else if (Files.isDirectory(jobFile)) {
            // TODO: Perhaps this should be different exception
            throw new IllegalArgumentException(relativePath + " is a directory not a file. Unable to update");
        }

        try (
            final FileChannel fileChannel = FileChannel.open(
                jobFile,
                EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.SPARSE)
            )
        ) {
            // Move the byteChannel to the start byte
            fileChannel.position(startByte);

            // The size and length are ignored in this implementation as we just assume we're writing everything atm
            // TODO: Would it be better to provide an input stream and buffer the output?
            final ByteBuffer byteBuffer = ByteBuffer.wrap(data);

            while (byteBuffer.hasRemaining()) {
                fileChannel.write(byteBuffer);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Resource getJobFileAsResource(final String jobId, @Nullable final String relativePath) {
        log.debug("Attempting to get resource for job {} with relative path {}", jobId, relativePath);
        final Path jobDirectoryLocation = StringUtils.isBlank(relativePath)
            ? this.jobsDirRoot.resolve(jobId)
            : this.jobsDirRoot.resolve(jobId).resolve(relativePath);
        return new PathResource(jobDirectoryLocation);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteJobFile(final String jobId, final String relativePath) throws IOException {
        log.debug("Requested to delete file {} for job {}", relativePath, jobId);
        final Path jobFile = this.jobsDirRoot.resolve(jobId).resolve(relativePath);

        if (Files.deleteIfExists(jobFile)) {
            log.debug("Deleted file {} for job {}", relativePath, jobId);
        } else {
            log.debug("No file {} exists for job {}. Ignoring", relativePath, jobId);
        }
    }

    private void createOrCheckDirectory(final Path dir) throws IOException {
        if (Files.notExists(dir)) {
            Files.createDirectories(dir);
        } else if (!Files.isDirectory(dir)) {
            // TODO: Maybe a more specific exception here? Should be runtime maybe as there's no recovery?
            throw new IOException(dir + " exists but is not a directory and must be");
        }
    }

    /**
     * This visitor will return immutable state about the files it visits and store them into the collection provided
     * in the constructor.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Slf4j
    static class FileStateVisitor implements FileVisitor<Path> {

        private final Path jobDirectoryRoot;
        private final boolean calculateMd5;
        private final Set<JobFileState> files;

        /**
         * Constructor.
         *
         * @param jobDirectoryRoot The original root of this visitor
         * @param calculateMd5     Whether or not an md5 of the file should be calculated during traversal
         * @param files            The result file metadata that should be added to by every call to
         *                         {@link #visitFile(Path, BasicFileAttributes)}
         */
        FileStateVisitor(final Path jobDirectoryRoot, final boolean calculateMd5, final Set<JobFileState> files) {
            this.jobDirectoryRoot = jobDirectoryRoot;
            this.calculateMd5 = calculateMd5;
            this.files = files;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            log.debug("Visiting file {}", file);
            final String relativePath = this.jobDirectoryRoot.relativize(file).toString();
            final long size = Files.size(file);
            final String md5;
            if (this.calculateMd5) {
                try (final InputStream fileInputStream = Files.newInputStream(file, StandardOpenOption.READ)) {
                    md5 = DigestUtils.md5Hex(fileInputStream);
                }
            } else {
                md5 = null;
            }

            final JobFileState jobFileState = new JobFileState(relativePath, size, md5);
            log.debug("Visited file has state {}", jobFileState);
            this.files.add(jobFileState);
            return FileVisitResult.CONTINUE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
            // TODO: What do we think best course of action here is
            return FileVisitResult.TERMINATE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }
    }
}
