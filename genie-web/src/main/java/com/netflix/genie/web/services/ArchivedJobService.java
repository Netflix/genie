/*
 *
 *  Copyright 2019 Netflix, Inc.
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

import com.netflix.genie.web.dtos.ArchivedJobMetadata;
import com.netflix.genie.web.exceptions.checked.JobDirectoryManifestNotFoundException;
import com.netflix.genie.web.exceptions.checked.JobNotArchivedException;
import com.netflix.genie.web.exceptions.checked.JobNotFoundException;

/**
 * A Service interface for working with the metadata and data of a job that was archived by the Agent upon completion.
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface ArchivedJobService {

    /**
     * Retrieve the metadata about the contents and location of a jobs archived artifacts.
     *
     * @param jobId The id of the job
     * @return A {@link ArchivedJobMetadata} instance
     * @throws JobNotFoundException                  When no job with id {@literal jobId} is found in the system
     * @throws JobNotArchivedException               If the job wasn't archived so no manifest could be retrieved
     * @throws JobDirectoryManifestNotFoundException If the job was archived but the manifest can't be located
     */
    ArchivedJobMetadata getArchivedJobMetadata(String jobId) throws
        JobNotFoundException,
        JobNotArchivedException,
        JobDirectoryManifestNotFoundException;
}
