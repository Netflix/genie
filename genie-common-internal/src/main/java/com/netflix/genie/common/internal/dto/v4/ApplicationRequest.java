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
package com.netflix.genie.common.internal.dto.v4;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.validation.Valid;

/**
 * Fields representing all the values users can set when creating a new Application resource.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@ToString(callSuper = true, doNotUseGetters = true)
@JsonDeserialize(builder = ApplicationRequest.Builder.class)
@SuppressWarnings("checkstyle:finalclass")
public class ApplicationRequest extends CommonRequestImpl {

    @Valid
    private final ApplicationMetadata metadata;

    private ApplicationRequest(final Builder builder) {
        super(builder);
        this.metadata = builder.bMetadata;
    }

    /**
     * Builder for a V4 Application Request.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder extends CommonRequestImpl.Builder<Builder> {

        private final ApplicationMetadata bMetadata;

        /**
         * Constructor which has required fields.
         *
         * @param metadata The user supplied metadata about an application resource
         */
        @JsonCreator
        public Builder(@JsonProperty(value = "metadata", required = true) final ApplicationMetadata metadata) {
            super();
            this.bMetadata = metadata;
        }

        /**
         * Build a new ApplicationRequest instance.
         *
         * @return The immutable application request
         */
        public ApplicationRequest build() {
            return new ApplicationRequest(this);
        }
    }
}
