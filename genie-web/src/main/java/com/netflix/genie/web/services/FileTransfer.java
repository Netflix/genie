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
package com.netflix.genie.web.services;

import com.netflix.genie.common.exceptions.GenieException;

/**
 * API to handle file transfer for genie jobs. There will be an implementation for different files systems
 * including local.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface FileTransfer {

    /**
     * This method validates whether it can handle operations for a particular file
     * based on the prefix of the file like s3://, hdfs:// etc.
     *
     * @param fileName file name to validate
     * @return whether the implementation can handle file based on prefix
     * @throws GenieException if there are errors
     */
    // TODO can we use a map of file system type to impl
    boolean isValid(final String fileName) throws GenieException;

    /**
     * Gets a file from any remote location to Genie's local working directory.
     *
     * @param srcRemotePath Source path of the files to copy
     * @param dstLocalPath  Destination path of the files to copy to
     * @throws GenieException exception in case of an error
     */
    void getFile(String srcRemotePath, String dstLocalPath) throws GenieException;

    /**
     * Puts a file from Genie's local working directory to a remote location.
     *
     * @param srcLocalPath  Source path of the files to copy
     * @param dstRemotePath Destination path of the files to copy to
     * @throws GenieException exception in case of an error
     */
    void putFile(String srcLocalPath, String dstRemotePath) throws GenieException;

    /**
     * Returns the last modified time of the file with the given path.
     *
     * @param path location of the file
     * @return time in milliseconds
     * @throws GenieException exception in case of IO error
     */
    long getLastModifiedTime(String path) throws GenieException;
}
