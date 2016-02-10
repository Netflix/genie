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
package com.netflix.genie.core.services;

import com.netflix.genie.common.exceptions.GenieException;

/**
 * API to handle file copying requested by genie jobs. There will be an implementation for different files systems
 * including local.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface FileCopyService {

    /**
     * This method validates whether it can handle copy operation for a particular file
     * based on the prefix of the file like s3://, hdfs:// etc.
     *
     * @param fileName file name to validate
     * @return where the implementation can handle file based on prefix
     * @throws GenieException if there are errors
     */
    // TODO can we use a map of file system type to impl
    boolean isValid(final String fileName) throws GenieException;

    /**
     * Copies the files from source to destination.
     *
     * @param srcPath Source path of the files to copy
     * @param destPath Destination path of the files to copy to
     * @throws GenieException exception in case of an error
     */
    void copy(final String srcPath, final String destPath) throws GenieException;

}
