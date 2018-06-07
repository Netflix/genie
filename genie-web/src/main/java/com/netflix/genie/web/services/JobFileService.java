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
package com.netflix.genie.web.services;

import com.netflix.genie.common.internal.dto.v4.files.JobFileState;
import org.springframework.core.io.Resource;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;

/**
 * A service for dealing with the files associated with a job run via Genie.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface JobFileService {

    /**
     * Create the root of the job directory for the given job.
     *
     * @param jobId The id of the job to create the directory for
     * @throws IOException When the job directory can't be created
     */
    void createJobDirectory(final String jobId) throws IOException;

    /**
     * Given a job id this API will traverse the job directory (if it exists) and return the set of all the files
     * including their relative paths from the directory root, the files' size and optionally a MD5 hash of the
     * file contents.
     *
     * @param jobId        The id of the job to get the job directory state for
     * @param calculateMd5 Whether to calculate the MD5 of the files or not. If true this will slow performance.
     * @return The set of {@link JobFileState} metadata records for all files in the directory
     * @throws IOException On error reading the directory
     */
    Set<JobFileState> getJobDirectoryFileState(final String jobId, final boolean calculateMd5) throws IOException;

    /**
     * Given a job id and a relative path for a file for that job write the data provided into the file.
     *
     * @param jobId        The id of the job this log file data belongs to
     * @param relativePath The relative path (from the log directory root) this file exists at
     * @param startByte    The starting byte of the data
     * @param data         The actual data to be written
     * @throws IOException On error writing the data to the given file
     */
    void updateFile(
        final String jobId,
        final String relativePath,
        final long startByte,
        final byte[] data
    ) throws IOException;

    /**
     * Given the expected path of a job resource (file or directory) for a given job return a {@link Resource}
     * handle for this location. A {@link Resource} has an {@link Resource#exists()} method that should be called to
     * ensure the underlying locations exists or not. No guarantee is made by implementations of this method that the
     * actual location exists.
     * <p>
     * Resource should not be written to outside the control of this service. In the future this
     * access may be cut off.
     *
     * @param jobId        The id of the job to get the log file resource for
     * @param relativePath The relative path of the log file from the root of the given jobs' log directory
     * @return The resource which may or may not actually exist. Use {@link Resource#exists()}
     */
    Resource getJobFileAsResource(final String jobId, @Nullable final String relativePath);

    /**
     * Given a job id this API will delete the entire file from the job directory this service implementation is
     * managing. If the job file doesn't actually exist this is a no-op.
     *
     * @param jobId        The id of the job to delete a file for
     * @param relativePath The relative path from the root of the job directory to delete
     * @throws IOException On error deleting the file
     */
    void deleteJobFile(final String jobId, final String relativePath) throws IOException;
}
