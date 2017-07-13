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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.FileTransfer;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * An implementation of the FileTransferService interface in which the remote locations are on local unix filesystem.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class LocalFileTransferImpl implements FileTransfer {

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
        log.debug("Called with src path {} and destination path {}", srcRemotePath, dstLocalPath);
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
        log.debug("Called with src path {} and destination path {}", srcLocalPath, dstRemotePath);
        this.copy(srcLocalPath, dstRemotePath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLastModifiedTime(final String path) throws GenieException {
        try {
            return new File(path).lastModified();
        } catch (Exception e) {
            final String message = String.format("Failed getting the last modified time for file with path %s", path);
            log.error(message, e);
            throw new GenieServerException(message, e);
        }
    }

    private void copy(final String srcPath, final String dstPath) throws GenieServerException {
        try {
            final File src = new File(srcPath);
            final File dest = new File(dstPath);
            if (!dest.getParentFile().exists()) {
                Files.createDirectories(dest.getParentFile().toPath());
            }
            Files.copy(src.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
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
}
