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
import org.springframework.validation.annotation.Validated;

import java.io.File;
import java.io.InputStream;

/**
 * APIs for dealing with attachments sent in with Genie jobs. Implementations will handle where to store them and
 * how to retrieve them when requested.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Validated
public interface AttachmentService {

    /**
     * Save a given attachment for a job for later retrieval.
     *
     * @param jobId    The id of the job to save the attachment for
     * @param filename The name of the attachment
     * @param content  A stream to access the contents of the attachment
     * @throws GenieException For any error during the save process
     */
    void save(final String jobId, final String filename, final InputStream content) throws GenieException;

    /**
     * Copy all the attachments for a job into the specified directory.
     *
     * @param jobId       The id of the job to get the attachments for.
     * @param destination The directory to copy the attachments into
     * @throws GenieException For any error during the copy process
     */
    void copy(final String jobId, final File destination) throws GenieException;

    /**
     * Delete the attachments for the given job.
     *
     * @param jobId The id of the job to delete the attachments for
     * @throws GenieException For any error during the delete process
     */
    void delete(final String jobId) throws GenieException;
}
