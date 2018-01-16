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
import lombok.Getter;

import javax.annotation.Nullable;
import javax.validation.Valid;
import java.util.Optional;

/**
 * Additional metadata associated with a Job Request such as client host, user agent, etc.
 *
 * @author tgianos
 * @since 3.0.0
 */
@JsonDeserialize(builder = JobMetadata.Builder.class)
@Getter
public class JobMetadata extends BaseDTO {

    private final String clientHost;
    private final String userAgent;
    private final Integer numAttachments;
    private final Long totalSizeOfAttachments;
    private final Long stdOutSize;
    private final Long stdErrSize;

    /**
     * Constructor used only through the builder.
     *
     * @param builder The builder to construct from
     */
    protected JobMetadata(@Valid final Builder builder) {
        super(builder);
        this.clientHost = builder.bClientHost;
        this.userAgent = builder.bUserAgent;
        this.numAttachments = builder.bNumAttachments;
        this.totalSizeOfAttachments = builder.bTotalSizeOfAttachments;
        this.stdOutSize = builder.bStdOutSize;
        this.stdErrSize = builder.bStdErrSize;
    }

    /**
     * Get the client host.
     *
     * @return Optional of the client host
     */
    public Optional<String> getClientHost() {
        return Optional.ofNullable(this.clientHost);
    }

    /**
     * Get the user agent.
     *
     * @return Optional of the user agent
     */
    public Optional<String> getUserAgent() {
        return Optional.ofNullable(this.userAgent);
    }

    /**
     * Get the number of attachments.
     *
     * @return The number of attachments as an optional
     */
    public Optional<Integer> getNumAttachments() {
        return Optional.ofNullable(this.numAttachments);
    }

    /**
     * Get the total size of the attachments.
     *
     * @return The total size of attachments as an optional
     */
    public Optional<Long> getTotalSizeOfAttachments() {
        return Optional.ofNullable(this.totalSizeOfAttachments);
    }

    /**
     * Get the size of standard out for this job.
     *
     * @return The size (in bytes) of this jobs standard out file as Optional
     */
    public Optional<Long> getStdOutSize() {
        return Optional.ofNullable(this.stdOutSize);
    }

    /**
     * Get the size of standard error for this job.
     *
     * @return The size (in bytes) of this jobs standard error file as Optional
     */
    public Optional<Long> getStdErrSize() {
        return Optional.ofNullable(this.stdErrSize);
    }

    /**
     * Builder for creating a JobMetadata instance.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends BaseDTO.Builder<Builder> {
        private String bClientHost;
        private String bUserAgent;
        private Integer bNumAttachments;
        private Long bTotalSizeOfAttachments;
        private Long bStdOutSize;
        private Long bStdErrSize;

        /**
         * Set the host name that sent the job request.
         *
         * @param clientHost The hostname to use.
         * @return The builder
         */
        public Builder withClientHost(@Nullable final String clientHost) {
            this.bClientHost = clientHost;
            return this;
        }

        /**
         * Set the user agent string the request came in with.
         *
         * @param userAgent The user agent string
         * @return The builder
         */
        public Builder withUserAgent(@Nullable final String userAgent) {
            this.bUserAgent = userAgent;
            return this;
        }

        /**
         * Set the number of attachments the job had.
         *
         * @param numAttachments The number of attachments sent in with the job request
         * @return The builder
         */
        public Builder withNumAttachments(@Nullable final Integer numAttachments) {
            this.bNumAttachments = numAttachments;
            return this;
        }

        /**
         * Set the total size (in bytes) of the attachments sent with the job request.
         *
         * @param totalSizeOfAttachments The total size of the attachments sent in with the job request
         * @return The builder
         */
        public Builder withTotalSizeOfAttachments(@Nullable final Long totalSizeOfAttachments) {
            this.bTotalSizeOfAttachments = totalSizeOfAttachments;
            return this;
        }

        /**
         * Set the total size (in bytes) of the jobs' standard out file.
         *
         * @param stdOutSize The total size of the jobs' standard out file
         * @return The builder
         */
        public Builder withStdOutSize(@Nullable final Long stdOutSize) {
            this.bStdOutSize = stdOutSize;
            return this;
        }

        /**
         * Set the total size (in bytes) of the jobs' standard error file.
         *
         * @param stdErrSize The total size of the jobs' standard error file
         * @return The builder
         */
        public Builder withStdErrSize(@Nullable final Long stdErrSize) {
            this.bStdErrSize = stdErrSize;
            return this;
        }

        /**
         * Create a new JobMetadata object from this builder.
         *
         * @return The JobMetadata read only instance
         */
        public JobMetadata build() {
            return new JobMetadata(this);
        }
    }
}
