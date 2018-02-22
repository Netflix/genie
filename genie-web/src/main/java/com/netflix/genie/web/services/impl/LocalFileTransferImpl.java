/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.amazonaws.util.StringUtils;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.web.services.FileTransfer;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * An implementation of the FileTransferService interface in which the remote locations are on local filesystem.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class LocalFileTransferImpl implements FileTransfer {

    private static final String FILE_SCHEME = "file:";
    private static final String ENTIRE_FILE_SCHEME = FILE_SCHEME + "//";

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(
        @NotBlank(message = "Filename cannot be blank") final String fileName
    ) throws GenieException {
        log.debug("Called with file name {}", fileName);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getFile(
        @NotBlank(message = "Source file path cannot be empty.") final String srcRemotePath,
        @NotBlank(message = "Destination local path cannot be empty") final String dstLocalPath
    ) throws GenieException {
        log.debug("Called to get {} and put it to {}", srcRemotePath, dstLocalPath);
        this.copy(srcRemotePath, dstLocalPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putFile(
        @NotBlank(message = "Source local path cannot be empty.") final String srcLocalPath,
        @NotBlank(message = "Destination remote path cannot be empty") final String dstRemotePath
    ) throws GenieException {
        log.debug("Called to take {} and put it to {}", srcLocalPath, dstRemotePath);
        this.copy(srcLocalPath, dstRemotePath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLastModifiedTime(final String path) throws GenieException {
        try {
            return Files.getLastModifiedTime(this.createFilePath(path)).toMillis();
        } catch (final Exception e) {
            final String message = String.format("Failed getting the last modified time for file with path %s", path);
            log.error(message, e);
            throw new GenieServerException(message, e);
        }
    }

    private void copy(final String srcPath, final String dstPath) throws GenieServerException {
        try {
            final Path src = this.createFilePath(srcPath);
            final Path dest = this.createFilePath(dstPath);
            final Path parent = dest.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }
            Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
        } catch (final IOException ioe) {
            log.error("Got error while copying file {} to {}", srcPath, dstPath, ioe);
            throw new GenieServerException(
                "Got error while copying file "
                    + srcPath
                    + " to "
                    + dstPath,
                ioe
            );
        }
    }

    private Path createFilePath(final String path) throws GenieServerException {
        log.debug("Normalizing path from {}", path);
        final String finalPath;
        if (StringUtils.beginsWithIgnoreCase(path, ENTIRE_FILE_SCHEME)) {
            finalPath = path;
        } else if (StringUtils.beginsWithIgnoreCase(path, FILE_SCHEME)) {
            finalPath = path.replace(FILE_SCHEME, ENTIRE_FILE_SCHEME);
        } else {
            finalPath = ENTIRE_FILE_SCHEME + path;
        }
        log.debug("Final path of {} after normalization is {}", path, finalPath);

        try {
            return Paths.get(new URI(finalPath));
        } catch (final IllegalArgumentException | URISyntaxException e) {
            log.error("Unable to convert {} to java.nio.file.Path due to {}", finalPath, e.getMessage(), e);
            throw new GenieServerException("Failed to get file path", e);
        }
    }
}
