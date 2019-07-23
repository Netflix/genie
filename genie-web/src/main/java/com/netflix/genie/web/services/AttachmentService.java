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
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import org.springframework.core.io.Resource;
import org.springframework.validation.annotation.Validated;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/**
 * APIs for dealing with attachments sent in with Genie requests. Implementations will handle where to store them and
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
     * @deprecated Use {@link #saveAll(Map)} instead
     */
    @Deprecated
    void save(String jobId, String filename, InputStream content) throws GenieException;

    /**
     * Copy all the attachments for a job into the specified directory.
     *
     * @param jobId       The id of the job to get the attachments for.
     * @param destination The directory to copy the attachments into
     * @throws GenieException For any error during the copy process
     * @deprecated Use {@link #copyAll(String, Path)} instead
     */
    @Deprecated
    void copy(String jobId, File destination) throws GenieException;

    /**
     * Delete the attachments for the given job.
     *
     * @param jobId The id of the job to delete the attachments for
     * @throws GenieException For any error during the delete process
     * @deprecated Use {@link #deleteAll(String)} instead
     */
    @Deprecated
    void delete(String jobId) throws GenieException;

    /**
     * Save all attachments for a given request.
     *
     * @param attachments The map of filename to contents for all attachments. All input streams will be closed after
     *                    this method returns
     * @return A unique identifier that can be used to reference the attachments later
     * @throws IOException If unable to save any of the attachments
     */
    String saveAll(Map<String, InputStream> attachments) throws IOException;

    /**
     * Copy all attachments associated with the given id into the provided {@literal destination}.
     *
     * @param id          The id that was returned from the original call to {@link #saveAll(Map)}
     * @param destination The destination where the attachments should be copied. Must be a directory if it already
     *                    exists. If it doesn't exist it will be created.
     * @throws IOException If the copy fails for any reason
     */
    void copyAll(String id, Path destination) throws IOException;

    /**
     * Delete all the attachments that were associated with the given {@literal id}.
     *
     * @param id The id that was returned from the original call to {@link #saveAll(Map)}
     * @throws IOException On error during deletion
     */
    void deleteAll(String id) throws IOException;

    /**
     * Given the id of a job and the set of attachments associated with that job this API should save the attachments
     * somewhere that the agent can access as job dependencies once the job runs.
     *
     * @param jobId       The id of the job these attachments are for
     * @param attachments The attachments sent by the user
     * @return The set of {@link URI} where the attachments were saved
     * @throws SaveAttachmentException on error when an attachment is attempted to be saved to the underlying storage
     */
    Set<URI> saveAttachments(String jobId, Set<Resource> attachments) throws SaveAttachmentException;

    /**
     * Given the id of a job delete all the attachments that were saved for it.
     *
     * @param jobId The id of the job to delete attachments for
     * @throws IOException on error while deleting the attachments
     */
    void deleteAttachments(String jobId) throws IOException;
}
