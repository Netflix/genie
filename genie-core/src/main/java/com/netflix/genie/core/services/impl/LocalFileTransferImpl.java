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
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.Executor;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An implementation of the FileTransferService interface in which the remote locations are on local unix filesystem.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
@Component
public class LocalFileTransferImpl implements FileTransfer {

    private final Pattern localPrefixPattern =
        Pattern.compile("^file://.*$");

    private final Executor executor;

    /**
     * Constructor.
     *
     * @param executor The executor to use to launch processes
     */
    @Autowired
    public LocalFileTransferImpl(
        final Executor executor
    ) {
        this.executor = executor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(
        @NotBlank(message = "Filename cannot be blank")
        final String fileName) throws GenieException {
        log.debug("Called with file name {}", fileName);
        final Matcher matcher =
            localPrefixPattern.matcher(fileName);
        return matcher.matches();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getFile(
        @NotBlank (message = "Source file path cannot be empty.")
        final String srcRemotePath,
        @NotBlank (message = "Destination local path cannot be empty")
        final String dstLocalPath
    ) throws GenieException {
        log.debug("Called with src path {} and destination path {}", srcRemotePath, dstLocalPath);
        final CommandLine commandLine = new CommandLine("cp");
        commandLine.addArgument(srcRemotePath);
        commandLine.addArgument(dstLocalPath);
        try {
            this.executor.execute(commandLine);
        } catch (IOException ioe) {
            log.error("Got error while copying remote file {} to local path {}", srcRemotePath, dstLocalPath);
            throw new GenieServerException(
                "Got error while copying remote file "
                + srcRemotePath
                + " to local path "
                + dstLocalPath, ioe);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putFile(
        @NotBlank (message = "Source local path cannot be empty.")
        final String srcLocalPath,
        @NotBlank (message = "Destination remote path cannot be empty")
        final String dstRemotePath
    ) throws GenieException {
        log.debug("Called with src path {} and destination path {}", srcLocalPath, dstRemotePath);
        final CommandLine commandLine = new CommandLine("cp");
        commandLine.addArgument(srcLocalPath);
        commandLine.addArgument(dstRemotePath);
        try {
            this.executor.execute(commandLine);
        } catch (IOException ioe) {
            log.error("Got error while copying remote file {} to local path {}", srcLocalPath, dstRemotePath);
            throw new GenieServerException(
                "Got error while copying remote file "
                    + dstRemotePath
                    + " to local path "
                    + srcLocalPath, ioe);
        }
    }
}
