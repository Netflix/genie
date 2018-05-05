/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.jpa.entities.projections;

import java.util.Optional;

/**
 * Projection of the jobs table which produces only the fields that were present in the pre-3.3.0
 * JobMetadata table before it was merged into one large jobs table.
 *
 * @author tgianos
 * @since 3.3.0
 */
public interface JobMetadataProjection extends AuditProjection {

    /**
     * Get the unique identifier of this job execution.
     *
     * @return The unique id
     */
    String getUniqueId();

    /**
     * Get the request api client hostname.
     *
     * @return {@link Optional} of the client host
     */
    Optional<String> getRequestApiClientHostname();

    /**
     * Get the user agent.
     *
     * @return Optional of the user agent
     */
    Optional<String> getRequestApiClientUserAgent();

    /**
     * Get the number of attachments.
     *
     * @return The number of attachments as an optional
     */
    Optional<Integer> getNumAttachments();

    /**
     * Get the total size of the attachments.
     *
     * @return The total size of attachments as an optional
     */
    Optional<Long> getTotalSizeOfAttachments();

    /**
     * Get the size of standard out for this job.
     *
     * @return The size (in bytes) of this jobs standard out file as Optional
     */
    Optional<Long> getStdOutSize();

    /**
     * Get the size of standard error for this job.
     *
     * @return The size (in bytes) of this jobs standard error file as Optional
     */
    Optional<Long> getStdErrSize();
}
