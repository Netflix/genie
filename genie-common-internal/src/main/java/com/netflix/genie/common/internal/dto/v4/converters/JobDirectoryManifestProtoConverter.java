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

package com.netflix.genie.common.internal.dto.v4.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.common.internal.dto.JobDirectoryManifest;
import com.netflix.genie.common.internal.exceptions.GenieConversionException;
import com.netflix.genie.proto.AgentManifestMessage;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import java.io.IOException;

/**
 * Converts {@link JobDirectoryManifest} from/to {@link AgentManifestMessage} in order to transport manifests
 * over gRPC.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Validated
public class JobDirectoryManifestProtoConverter {

    private final ObjectMapper objectMapper;

    /**
     * Constructor.
     *
     * @param objectMapper an object mapper
     */
    public JobDirectoryManifestProtoConverter(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Construct a {@link AgentManifestMessage} from the given {@link JobDirectoryManifest}.
     *
     * @param claimedJobId the id of the job this file manifest belongs to
     * @param manifest     the manifest
     * @return a {@link AgentManifestMessage}
     * @throws GenieConversionException if conversion fails
     */
    public AgentManifestMessage manifestToProtoMessage(
        @NotBlank final String claimedJobId,
        final JobDirectoryManifest manifest
    ) throws GenieConversionException {
        final String manifestJsonString;
        try {
            // Leverage the existing manifest serialization to JSON rather than creating and using a protobuf schema.
            manifestJsonString = objectMapper.writeValueAsString(manifest);
        } catch (final JsonProcessingException e) {
            throw new GenieConversionException("Failed to serialize manifest as JSON string", e);
        }

        return AgentManifestMessage.newBuilder()
            .setJobId(claimedJobId)
            .setManifestJson(manifestJsonString)
            .build();
    }

    /**
     * Load a {@link JobDirectoryManifest} from a {@link AgentManifestMessage}.
     *
     * @param message the message
     * @return a {@link JobDirectoryManifest}
     * @throws GenieConversionException if loading fails
     */
    public JobDirectoryManifest toManifest(final AgentManifestMessage message) throws GenieConversionException {
        try {
            return objectMapper.readValue(message.getManifestJson(), JobDirectoryManifest.class);
        } catch (final IOException e) {
            throw new GenieConversionException("Failed to load manifest", e);
        }
    }
}
