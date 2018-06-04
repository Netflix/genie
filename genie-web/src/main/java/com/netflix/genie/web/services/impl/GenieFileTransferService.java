/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.web.services.FileTransfer;
import com.netflix.genie.web.services.FileTransferFactory;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class abstracts away all the implementations of FileTransfer interface. It iterates through a list of
 * available implementations and tries to perform the file transfer operations.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class GenieFileTransferService {
    private static final String BEAN_NAME_FILE_SYSTEM_PREFIX = "file.system.";
    private final FileTransferFactory fileTransferFactory;

    /**
     * Constructor.
     *
     * @param fileTransferFactory file transfer implementation factory
     * @throws GenieException If there is any problem
     */
    public GenieFileTransferService(@NotNull final FileTransferFactory fileTransferFactory) throws GenieException {
        this.fileTransferFactory = fileTransferFactory;
    }

    /**
     * Get the file needed by Genie for job execution.
     *
     * @param srcRemotePath Path of the file in the remote location to be fetched
     * @param dstLocalPath  Local path where the file needs to be placed
     * @throws GenieException If there is any problem
     */
    public void getFile(
        @NotBlank(message = "Source file path cannot be empty.") final String srcRemotePath,
        @NotBlank(message = "Destination local path cannot be empty") final String dstLocalPath
    ) throws GenieException {
        log.debug("Called with src path {} and destination path {}", srcRemotePath, dstLocalPath);

        this.getFileTransfer(srcRemotePath).getFile(srcRemotePath, dstLocalPath);
    }

    /**
     * Put the file provided by Genie.
     *
     * @param srcLocalPath  The local path of the file which has to be transfered to remote location
     * @param dstRemotePath The remote destination path where the file has to be put
     * @throws GenieException If there is any problem
     */
    public void putFile(
        @NotBlank(message = "Source local path cannot be empty.") final String srcLocalPath,
        @NotBlank(message = "Destination remote path cannot be empty") final String dstRemotePath
    ) throws GenieException {
        log.debug("Called with src path {} and destination path {}", srcLocalPath, dstRemotePath);

        this.getFileTransfer(dstRemotePath).putFile(srcLocalPath, dstRemotePath);
    }

    FileTransfer getFileTransfer(final String path) throws GenieNotFoundException {
        final FileTransfer result;
        try {
            final URI uri = new URI(path);
            result = fileTransferFactory.get(BEAN_NAME_FILE_SYSTEM_PREFIX + uri.getScheme());
        } catch (URISyntaxException ignored) {
            throw new GenieNotFoundException("Invalid file path: " + path, ignored);
        }
        if (result == null) {
            throw new GenieNotFoundException("Failed getting the appropriate FileTransfer implementation to get file: "
                + path);
        }
        return result;
    }
}
