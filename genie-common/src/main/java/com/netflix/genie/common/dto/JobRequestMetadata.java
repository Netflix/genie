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
package com.netflix.genie.common.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.validation.Valid;
import javax.validation.constraints.Min;

/**
 * Additional metadata associated with a Job Request such as client host, user agent, etc.
 *
 * @author tgianos
 * @since 3.0.0
 */
@JsonDeserialize(builder = JobRequestMetadata.Builder.class)
@Getter
@EqualsAndHashCode
@ToString
public class JobRequestMetadata {

    private final String clientHost;
    private final String userAgent;
    @Min(value = 0, message = "Can't have less than zero attachments")
    private final int numAttachments;
    @Min(value = 0, message = "Can't have a size of less than zero bytes")
    private final long totalSizeOfAttachments;

    /**
     * Constructor used only through the builder.
     *
     * @param builder The builder to construct from
     */
    protected JobRequestMetadata(@Valid final Builder builder) {
        this.clientHost = builder.bClientHost;
        this.userAgent = builder.bUserAgent;
        this.numAttachments = builder.bNumAttachments;
        this.totalSizeOfAttachments = builder.bTotalSizeOfAttachments;
    }

    /**
     * Builder for creating a JobRequestMetadata instance.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder {
        private String bClientHost;
        private String bUserAgent;
        @Min(value = 0, message = "Can't have less than zero attachments")
        private int bNumAttachments;
        @Min(value = 0, message = "Can't have a size of less than zero bytes")
        private long bTotalSizeOfAttachments;

        /**
         * Set the host name that sent the job request.
         *
         * @param clientHost The hostname to use.
         * @return The builder
         */
        public Builder withClientHost(final String clientHost) {
            this.bClientHost = clientHost;
            return this;
        }

        /**
         * Set the user agent string the request came in with.
         *
         * @param userAgent The user agent string
         * @return The builder
         */
        public Builder withUserAgent(final String userAgent) {
            this.bUserAgent = userAgent;
            return this;
        }

        /**
         * Set the number of attachments the job had.
         *
         * @param numAttachments The number of attachments sent in with the job request
         * @return The builder
         */
        public Builder withNumAttachments(@Min(0) final int numAttachments) {
            this.bNumAttachments = numAttachments;
            return this;
        }

        /**
         * Set the total size (in bytes) of the attachments sent with the job request.
         *
         * @param totalSizeOfAttachments The total size of the attachments sent in with the job request
         * @return The builder
         */
        public Builder withTotalSizeOfAttachments(@Min(0) final long totalSizeOfAttachments) {
            this.bTotalSizeOfAttachments = totalSizeOfAttachments;
            return this;
        }

        /**
         * Create a new JobRequestMetadata object from this builder.
         *
         * @return The JobRequestMetadata read only instance
         */
        public JobRequestMetadata build() {
            return new JobRequestMetadata(this);
        }
    }
}
