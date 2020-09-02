/*
 *
 *  Copyright 2020 Netflix, Inc.
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

import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import org.springframework.core.io.Resource;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Set;

/**
 * APIs for saving a job attachments sent in with Genie requests.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Validated
public interface AttachmentService {

    /**
     * Save the attachments and return their URIs so agent executing the job can retrieve them.
     *
     * @param jobId       The id of the job these attachments are for, if one was present in the job request
     *                    This is strictly for debugging and logging.
     * @param attachments The attachments sent by the user
     * @return The set of {@link URI} which can be used to retrieve the attachments
     * @throws SaveAttachmentException if an error is encountered while saving
     */
    Set<URI> saveAttachments(@Nullable String jobId, Set<Resource> attachments) throws SaveAttachmentException;

}
