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
import com.netflix.genie.common.dto.CommandStatus;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.validation.constraints.NotNull;

/**
 * Metadata supplied by a user for a Command resource.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(callSuper = true, doNotUseGetters = true)
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@JsonDeserialize(builder = CommandMetadata.Builder.class)
@SuppressWarnings("checkstyle:finalclass")
public class CommandMetadata extends CommonMetadata {

    @NotNull(message = "A command status is required")
    private final CommandStatus status;

    private CommandMetadata(final Builder builder) {
        super(builder);
        this.status = builder.bStatus;
    }


    /**
     * A builder to create command user metadata instances.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder extends CommonMetadata.Builder<Builder> {

        private final CommandStatus bStatus;

        /**
         * Constructor which has required fields.
         *
         * @param name    The name to use for the command
         * @param user    The user who owns the command
         * @param version The version of the command
         * @param status  The status of the command
         */
        @JsonCreator
        public Builder(
            @JsonProperty(value = "name", required = true) final String name,
            @JsonProperty(value = "user", required = true) final String user,
            @JsonProperty(value = "version", required = true) final String version,
            @JsonProperty(value = "status", required = true) final CommandStatus status
        ) {
            super(name, user, version);
            this.bStatus = status;
        }

        /**
         * Build the command metadata instance.
         *
         * @return Create the final read-only commandMetadata instance
         */
        public CommandMetadata build() {
            return new CommandMetadata(this);
        }
    }
}
