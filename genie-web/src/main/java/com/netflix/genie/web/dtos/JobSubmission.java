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
package com.netflix.genie.web.dtos;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

/**
 * The payload of all gathered information from a user request to run a job via the API.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
@SuppressWarnings("FinalClass")
public class JobSubmission {

    @NotNull
    @Valid
    private final JobRequest jobRequest;
    @NotNull
    @Valid
    private final JobRequestMetadata jobRequestMetadata;
    @NotNull
    private final Set<URI> attachments;

    private JobSubmission(final Builder builder) {
        this.jobRequest = builder.bJobRequest;
        this.jobRequestMetadata = builder.bJobRequestMetadata;
        this.attachments = ImmutableSet.copyOf(builder.bAttachments);
    }

    /**
     * Builder for {@link JobSubmission} instances.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder {
        private final JobRequest bJobRequest;
        private final JobRequestMetadata bJobRequestMetadata;
        private final Set<URI> bAttachments;

        /**
         * Constructor with required parameters.
         *
         * @param jobRequest         The job request metadata entered by the user
         * @param jobRequestMetadata The metadata collected by the system about the request
         */
        public Builder(final JobRequest jobRequest, final JobRequestMetadata jobRequestMetadata) {
            this.bJobRequest = jobRequest;
            this.bJobRequestMetadata = jobRequestMetadata;
            this.bAttachments = Sets.newHashSet();
        }

        /**
         * Set the attachments associated with this submission if there were any.
         *
         * @param attachments The attachments {@link URI}s
         * @return the builder
         */
        public Builder withAttachments(@Nullable final Set<URI> attachments) {
            this.setAttachments(attachments);
            return this;
        }

        /**
         * Set the attachments associated with this submission.
         *
         * @param attachments The attachments as {@link URI}s
         * @return the builder
         */
        public Builder withAttachments(final URI... attachments) {
            this.setAttachments(Arrays.asList(attachments));
            return this;
        }

        /**
         * Build an immutable {@link JobSubmission} instance based on the current contents of this builder.
         *
         * @return An {@link JobSubmission} instance
         */
        public JobSubmission build() {
            return new JobSubmission(this);
        }

        private void setAttachments(@Nullable final Collection<URI> attachments) {
            this.bAttachments.clear();
            if (attachments != null) {
                this.bAttachments.addAll(attachments);
            }
        }
    }
}
