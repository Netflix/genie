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

package com.netflix.genie.agent.execution.services.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.netflix.genie.agent.execution.exceptions.ArchivalException;
import com.netflix.genie.agent.execution.services.ArchivalService;
import com.netflix.genie.common.internal.jobs.JobConstants;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URI;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Implementation of ArchivalService for S3.
 *
 * @author standon
 * @since 4.0.0
 */
@Slf4j
class S3ArchivalServiceImpl implements ArchivalService {

    private final AmazonS3 amazonS3;

    S3ArchivalServiceImpl(final AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
    }

    /**
     * Archive a regular file or recursively archive a directory.
     * For a directory all its contents are recursively and each file is
     * stored as an object in S3. Each empty directory is also stored as an object with
     * empty content. The key for storing an empty directory has a trailing /
     * <p>
     * Example 1
     * <p>
     * For a dir hierarchy
     * /tmp/foo/emptyDir
     * /tmp/foo/file1
     * /tmp/foo/bar/file2
     * <p>
     * dir to be archived - /tmp/foo
     * target uri - s3://bucketName/foo
     * <p>
     * following structure is created in S3
     * s3://bucketName/foo/emptyDir/ -> empty content
     * s3://bucketName/foo/file1 -> contents of file1
     * s3://bucketName/foo/bar/file2 -> contents of file2
     * <p>
     * Example 2
     * <p>
     * file to archive - /tmp/file1
     * target uri - s3://bucketName/file1
     * <p>
     * following structure is created in S3
     * s3://bucketName/file1 -> contents of file1
     *
     * @param path      path to file/directory to archive.
     * @param targetURI s3 archival location uri for the file/dir
     * @throws ArchivalException if archival fails
     */
    @Override
    public void archive(
        @NotNull final Path path,
        @NotNull final URI targetURI
    ) throws ArchivalException {
        log.info("Archiving to location: {}", targetURI);
        try {
            Files.walkFileTree(path, new FileArchivalVisitor(path, amazonS3, new AmazonS3URI(targetURI)));
        } catch (Exception e) {
            log.info("Error archiving to location: {} ", targetURI);
            throw new ArchivalException("Error archiving file: " + path.getFileName(), e);
        }
    }

    /**
     * This visitor will archive regular files and empty directories to a target S3 location.
     * No action for non empty directories.
     */
    static class FileArchivalVisitor implements FileVisitor<Path> {
        private final Path root;
        private final AmazonS3 amazonS3;
        private final AmazonS3URI rootTargetURI;

        /**
         * Constructor.
         * @param root          Root of the directory being archived. For a regular file, path to the file
         * @param amazonS3      S3 client
         * @param rootTargetURI S3 uri where root should get archived
         */
        FileArchivalVisitor(final Path root, final AmazonS3 amazonS3, final AmazonS3URI rootTargetURI) {
            this.root = root;
            this.amazonS3 = amazonS3;
            this.rootTargetURI = rootTargetURI;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            final File[] children = dir.toFile().listFiles();
            if (children != null && children.length == 0) { //empty dir
                final AmazonS3URI targetURI = getTargetURI(dir);
                //create meta data with 0 content length
                final ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(0);

                //create a PutObjectRequest passing the folder name with a trailing File.separator
                //as the key
                final PutObjectRequest putObjectRequest = new PutObjectRequest(
                    targetURI.getBucket(),
                    targetURI.getKey() + File.separator,
                    new ByteArrayInputStream(JobConstants.EMPTY_BYTE_ARRAY), metadata);

                amazonS3.putObject(putObjectRequest);
            }

            return FileVisitResult.CONTINUE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) throws IOException {
            final AmazonS3URI targetURI = getTargetURI(path);
            amazonS3.putObject(
                new PutObjectRequest(
                    targetURI.getBucket(),
                    targetURI.getKey(),
                    path.toFile()
                )
            );
            return FileVisitResult.CONTINUE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
            return FileVisitResult.TERMINATE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        private AmazonS3URI getTargetURI(final Path path) {
            return new AmazonS3URI(rootTargetURI + File.separator + root.relativize(path));
        }
    }
}
