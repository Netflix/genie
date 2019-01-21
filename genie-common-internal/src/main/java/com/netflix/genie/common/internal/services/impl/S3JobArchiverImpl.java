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

package com.netflix.genie.common.internal.services.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.exceptions.JobArchiveException;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.JobArchiver;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Implementation of {@link JobArchiveService} for S3 destinations.
 *
 * @author standon
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class S3JobArchiverImpl implements JobArchiver {

    private final S3ClientFactory s3ClientFactory;

    /**
     * Constructor.
     *
     * @param s3ClientFactory The factory to use to get S3 client instances for a given S3 bucket.
     */
    public S3JobArchiverImpl(final S3ClientFactory s3ClientFactory) {
        this.s3ClientFactory = s3ClientFactory;
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
     * s3://bucketName/foo/emptyDir/ contains empty content
     * s3://bucketName/foo/file1 contains contents of file1
     * s3://bucketName/foo/bar/file2 contains contents of file2
     * <p>
     * Example 2
     * <p>
     * file to archive - /tmp/file1
     * target uri - s3://bucketName/file1
     * <p>
     * following structure is created in S3
     * s3://bucketName/file1 contains contents of file1
     *
     * @param directory path to file/directory to archive.
     * @param target    s3 archival location uri for the file/dir
     * @throws JobArchiveException if archival fails
     */
    @Override
    public boolean archiveDirectory(
        @NotNull final Path directory,
        @NotNull final URI target
    ) throws JobArchiveException {
        final String uriString = target.toString();
        final String directoryString = directory.toString();
        final AmazonS3URI s3URI;
        try {
            s3URI = new AmazonS3URI(target);
        } catch (final IllegalArgumentException iae) {
            log.debug("{} is not a valid S3 URI", uriString);
            return false;
        }
        log.debug(
            "{} is a valid S3 location. Proceeding to archiving {} to location: {}",
            directoryString,
            uriString
        );

        try {
            Files.walkFileTree(
                directory,
                new FileArchivalVisitor(directory, this.s3ClientFactory.getClient(s3URI), s3URI)
            );
            return true;
        } catch (final Exception e) {
            log.error("Error archiving to S3 location: {} ", uriString, e);
            throw new JobArchiveException("Error archiving " + directoryString, e);
        }
    }

    /**
     * This visitor will archiveDirectory regular files and empty directories to a target S3 location.
     * No action for non empty directories.
     */
    static class FileArchivalVisitor implements FileVisitor<Path> {
        private final Path root;
        private final AmazonS3 amazonS3;
        private final AmazonS3URI rootTargetURI;

        /**
         * Constructor.
         *
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
