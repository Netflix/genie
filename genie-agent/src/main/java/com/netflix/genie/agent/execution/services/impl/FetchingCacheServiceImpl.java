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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;
import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.execution.exceptions.DownloadException;
import com.netflix.genie.agent.execution.exceptions.LockException;
import com.netflix.genie.agent.utils.locks.CloseableLock;
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory;
import com.netflix.genie.agent.execution.services.FetchingCacheService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * A cache on local disk that uses URIs as keys and transparently downloads
 * missing entries. It uses file locks to accomplish the following
 * <p>
 * Allows multiple agents reading/writing/deleting in the same cache folder
 * without corrupting the cache
 * Downloads new versions of resources once they are available remotely
 * Deletes the old versions of resources once a new version is successfully downloaded
 * Recovers from partial downloads of resources in case an agent gets killed in the middle of a download
 * or a download fails for any other reason
 * <p>
 * Does NOT handle concurrency within the same agent (not an issue at the moment)
 * <p>
 * Cache structure on local disk
 * Each resource has a hash to represent it. The version of the resource is the remote last modified
 * timestamp. For each resource and version, a lock file is created. Each process takes a lock on
 * this file and then downloads the resource to a data.tmp file. Once the download is complete
 * data.tmp is atomically moved to data.
 * For e.g for a hash value of 6d331abc92bc8244bc5d41e2107f303a and last modified = 1525456404
 * cache entries would like the following
 * {base_dir}/6d331abc92bc8244bc5d41e2107f303a/1525456404/data
 * {base_dir}/6d331abc92bc8244bc5d41e2107f303a/1525456404/lock
 * <p>
 * Deletion of older versions
 * Once a version is successfully downloaded, any older versions are deleted as a best effort
 * TODO:Use shared file lock for reading and exclusive lock for writing to the cache
 *
 * @author standon
 * @since 4.0.0
 */
@Service
@Lazy
@Slf4j
@Component
class FetchingCacheServiceImpl implements FetchingCacheService {

    private static final String LOCK_FILE_NAME = "lock";
    private static final String DATA_FILE_NAME = "data";
    private static final String DATA_DOWNLOAD_FILE_NAME = "data.tmp";
    private static final String DUMMY_FILE_NAME = "_";
    private final ResourceLoader resourceLoader;
    private final File cacheDirectory;
    private final FileLockFactory fileLockFactory;
    private final TaskExecutor cleanUpTaskExecutor;

    FetchingCacheServiceImpl(
        final ResourceLoader resourceLoader,
        final ArgumentDelegates.CacheArguments cacheArguments,
        final FileLockFactory fileLockFactory,
        @Qualifier("fetchingCacheServiceCleanUpTaskExecutor") final TaskExecutor cleanUpTaskExecutor
    ) throws IOException {
        this.resourceLoader = resourceLoader;
        this.cacheDirectory = cacheArguments.getCacheDirectory();
        this.fileLockFactory = fileLockFactory;
        this.cleanUpTaskExecutor = cleanUpTaskExecutor;
        createDirectoryStructureIfNotExists(cacheDirectory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void get(final URI sourceFileUri, final File destinationFile) throws DownloadException, IOException {
        lookupOrDownload(sourceFileUri, destinationFile);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void get(final Set<Pair<URI, File>> sourceDestinationPairs) throws DownloadException, IOException {
        for (final Pair<URI, File> sourceDestinationPair : sourceDestinationPairs) {
            get(sourceDestinationPair.getKey(), sourceDestinationPair.getValue());
        }
    }

    /* Get a handle to the resource represented by the sourceFileURI.
     * The lastModifedTimeStamp represents the version number of the resource.
     * Create the directory structure with resourceCacheId/version(lastModifiedTimeStamp)
     * if it does not exist. Touch an empty lock file. Use this file to grab a lock on it.
     * While under the lock check for the cache data file resourceCacheId/version/DATA_FILE_NAME.
     * If it exists copy to the target file and release the lock. Else,
     * download the file to resourceCacheId/version/DATA_DOWNLOAD_FILE_NAME download file. Move it
     * to the data file(this operation is guaranteed to be atomic by the OS). Copy data
     * file to target file and release the lock.
     * Before exiting delete the previous versions of the resource
     */
    private void lookupOrDownload(
        final URI sourceFileUri,
        final File destinationFile
    ) throws DownloadException, IOException {

        final String uriString = sourceFileUri.toASCIIString();

        log.debug("Lookup: {}", uriString);

        // Unique id to store the resource on local disk
        final String resourceCacheId = getResourceCacheId(sourceFileUri);

        // Get a handle to the resource
        final Resource resource = resourceLoader.getResource(uriString);

        if (!resource.exists()) {
            throw new DownloadException("Resource not found: " + uriString);
        }

        final long resourceLastModified = resource.lastModified();

        //Handle to resourceCacheId/version
        final File cacheResourceVersionDir = getCacheResourceVersionDir(
            resourceCacheId,
            resourceLastModified
        );

        //Create the resource version dir in cache if it does not exist
        createDirectoryStructureIfNotExists(cacheResourceVersionDir);

        try (
            final CloseableLock lock = fileLockFactory.getLock(
                touchCacheResourceVersionLockFile(
                    resourceCacheId,
                    resourceLastModified
                )
            );
        ) {
            //Critical section begin
            lock.lock();

            //Handle to the resource cached locally
            final File cachedResourceVersionDataFile = getCacheResourceVersionDataFile(
                resourceCacheId,
                resourceLastModified
            );

            if (!cachedResourceVersionDataFile.exists()) {
                log.debug(
                    "Cache miss: {} (id: {})",
                    uriString,
                    resourceCacheId
                );

                // Download the resource into the download file in cache
                // resourceCacheId/version/data.tmp
                final File cachedResourceVersionDownloadFile = getCacheResourceVersionDownloadFile(
                    resourceCacheId,
                    resourceLastModified
                );
                try (
                    final InputStream in = resource.getInputStream();
                    final OutputStream out = new FileOutputStream(cachedResourceVersionDownloadFile)
                ) {
                    FileCopyUtils.copy(in, out);
                    Files.move(cachedResourceVersionDownloadFile, cachedResourceVersionDataFile);
                }
            } else {
                log.debug(
                    "Cache hit: {} (id: {})",
                    uriString,
                    resourceCacheId
                );
            }

            //Copy from cache data file resourceCacheId/version/DATA_FILE_NAME to targetFile
            Files.copy(cachedResourceVersionDataFile, destinationFile);
            //Critical section end
        } catch (LockException e) {
            throw new DownloadException("Error downloading dependency", e);
        }

        //Clean up any older versions
        cleanUpTaskExecutor.execute(
            new CleanupOlderVersionsTask(resourceCacheId, resourceLastModified)
        );
    }

    @VisibleForTesting
    String getResourceCacheId(final URI uri) {
        return DigestUtils.md5DigestAsHex(uri.toASCIIString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Helper to extract the resource last modified timestamp from the resource
     * version directory handle.
     *
     * @param resourceVersionDir handle to the resource version directory
     * @return resource last modified timestamp
     */
    private long getResourceLastModified(final File resourceVersionDir) {
        return Long.parseLong(resourceVersionDir.getName());
    }

    private void createDirectoryStructureIfNotExists(final File dir) throws IOException {
        if (!dir.exists()) {
            try {
                Files.createParentDirs(new File(dir, DUMMY_FILE_NAME));
            } catch (final IOException e) {
                throw new IOException("Failed to create directory: " + dir.getAbsolutePath(), e);
            }
        } else if (!dir.isDirectory()) {
            throw new IOException("This location is not a directory: " + dir.getAbsolutePath());
        }
    }

    /**
     * Clean up all resources older than the latest version of a resource.
     *
     * @param resourceCacheId                         resource cache id
     * @param lastDownloadedResourceModifiedTimestamp timestamp of last successfully downloaded resource.
     *                                                represents the latest version of the resource
     * @throws IOException in case deleting the files has an issue
     */
    @VisibleForTesting
    void cleanUpOlderResourceVersions(
        final String resourceCacheId,
        final long lastDownloadedResourceModifiedTimestamp
    ) throws IOException, LockException {

        //Get all versions of a resource in the cache
        final File[] files = getCacheResourceDir(resourceCacheId).listFiles();

        //Remove all the versions older than the supplied version - lastDownloadedResourceModifiedTimestamp
        if (files != null) {
            for (File file : files) {
                long resourceLastModified = 0;
                try {
                    resourceLastModified = getResourceLastModified(file);
                    if (resourceLastModified < lastDownloadedResourceModifiedTimestamp) {
                        cleanUpResourceVersion(file);
                    }
                } catch (NumberFormatException e) {
                    log.warn("Encountered a dir name which is not long. Ignoring dir - {}", resourceLastModified, e);
                }
            }
        }
    }

    /**
     * Delete a resource version directory after taking appropriate lock.
     *
     * @param resourceVersionDir Directory to be deleted
     * @throws IOException
     */
    private void cleanUpResourceVersion(final File resourceVersionDir)
        throws LockException, IOException {
        /*
         * Acquire a lock on the lock file for the resource version being deleted.
         * Delete the entire directory for the resource version
         */
        try (
            final CloseableLock lock = fileLockFactory.getLock(
                touchCacheResourceVersionLockFile(resourceVersionDir)
            );
        ) {
            //critical section begin
            lock.lock();

            //Remove the data file. If last download was successful for the resource, only
            //data file would exist
            FileSystemUtils.deleteRecursively(getCacheResourceVersionDataFile(resourceVersionDir));

            //data.tmp file could exist if the last download of the resource failed in the middle
            //and after that a newer version was downloaded. So, delete it too
            FileSystemUtils.deleteRecursively(getCacheResourceVersionDownloadFile(resourceVersionDir));

            //critical section end
        }
    }

    /* Returns a handle to the directory for a resource */
    private File getCacheResourceDir(final String resourceCacheId) {
        return new File(cacheDirectory, resourceCacheId);
    }

    /* Returns a handle to the directory for a resource version */
    @VisibleForTesting
    File getCacheResourceVersionDir(final String resourceCacheId, final long lastModifiedTimestamp) {
        return new File(getCacheResourceDir(resourceCacheId), Long.toString(lastModifiedTimestamp));
    }

    /* Returns a handle to the data file of a resource version in the cache */
    @VisibleForTesting
    File getCacheResourceVersionDataFile(final String resourceCacheId, final long lastModifiedTimestamp) {
        return getCacheResourceVersionDataFile(
            getCacheResourceVersionDir(resourceCacheId, lastModifiedTimestamp)
        );
    }

    /* Returns a handle to the data file of a resource version in the cache */
    @VisibleForTesting
    File getCacheResourceVersionDataFile(final File resourceVersionDir) {
        return new File(resourceVersionDir, DATA_FILE_NAME);
    }

    /* Returns a handle to the temporary download file of a resource version in the cache */
    @VisibleForTesting
    File getCacheResourceVersionDownloadFile(final String resourceCacheId, final long lastModifiedTimestamp) {
        return getCacheResourceVersionDownloadFile(
            getCacheResourceVersionDir(resourceCacheId, lastModifiedTimestamp)
        );
    }

    /* Returns a handle to the temporary download file of a resource version in the cache */
    @VisibleForTesting
    File getCacheResourceVersionDownloadFile(final File resourceVersionDir) {
        return new File(resourceVersionDir, DATA_DOWNLOAD_FILE_NAME);
    }

    /* Returns a handle to the lock file of a resource version in the cache */
    @VisibleForTesting
    File getCacheResourceVersionLockFile(final String resourceCacheId, final long lastModifiedTimestamp) {
        return getCacheResourceVersionLockFile(
            getCacheResourceVersionDir(
                resourceCacheId, lastModifiedTimestamp
            )
        );
    }

    private File getCacheResourceVersionLockFile(final File resourceVersionDir) {
        return new File(resourceVersionDir, LOCK_FILE_NAME);
    }

    /* Touch the lock file of a resource version and return a handle to it */
    File touchCacheResourceVersionLockFile(
        final String resourceCacheId,
        final long lastModifiedTimestamp
    ) throws IOException {
        return touchCacheResourceVersionLockFile(
            getCacheResourceVersionDir(
                resourceCacheId,
                lastModifiedTimestamp
            )
        );
    }

    /* Touch the lock file of a resource version and return a handle to it */
    private File touchCacheResourceVersionLockFile(final File resourceVersionDir) throws IOException {
        final File lockFile = getCacheResourceVersionLockFile(resourceVersionDir);
        Files.touch(lockFile);
        return lockFile;
    }

    /**
     * Task to clean up the older versions of a resource.
     */
    private class CleanupOlderVersionsTask implements Runnable {

        //Cache id for the resource
        private final String resourceCacheId;
        //lastModified timestamp for the last successfully downloaded resource
        private final long lastDownloadedResourceModifiedTimestamp;

        CleanupOlderVersionsTask(final String resourceCacheId, final long lastDownloadedResourceModifiedTimestamp) {
            this.resourceCacheId = resourceCacheId;
            this.lastDownloadedResourceModifiedTimestamp = lastDownloadedResourceModifiedTimestamp;
        }

        @Override
        public void run() {
            try {
                cleanUpOlderResourceVersions(resourceCacheId, lastDownloadedResourceModifiedTimestamp);
            } catch (Throwable throwable) {
                log.error(
                    "Error cleaning up old resource resourceCacheId - {}, resourceLastModified - {}",
                    resourceCacheId,
                    lastDownloadedResourceModifiedTimestamp,
                    throwable
                );
            }
        }
    }
}
