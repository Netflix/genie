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
package com.netflix.genie.web.jpa.entities.v4;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.v4.Application;
import com.netflix.genie.common.dto.v4.ApplicationMetadata;
import com.netflix.genie.common.dto.v4.Cluster;
import com.netflix.genie.common.dto.v4.ClusterMetadata;
import com.netflix.genie.common.dto.v4.Command;
import com.netflix.genie.common.dto.v4.CommandMetadata;
import com.netflix.genie.common.dto.v4.CommonMetadata;
import com.netflix.genie.common.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.web.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.jpa.entities.ClusterEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.jpa.entities.projections.BaseProjection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.stream.Collectors;

/**
 * Utility methods for converting from V4 DTO to entities and vice versa.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public final class EntityDtoConverters {

    private EntityDtoConverters() {
    }

    /**
     * Convert an application entity to a DTO for external exposure.
     *
     * @param applicationEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     */
    static Application toV4ApplicationDto(final ApplicationEntity applicationEntity) {
        final ApplicationMetadata.Builder metadataBuilder = new ApplicationMetadata.Builder(
            applicationEntity.getName(),
            applicationEntity.getUser(),
            applicationEntity.getStatus()
        )
            .withVersion(applicationEntity.getVersion())
            .withTags(applicationEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()));

        applicationEntity.getType().ifPresent(metadataBuilder::withType);
        applicationEntity.getDescription().ifPresent(metadataBuilder::withDescription);
        setDtoMetadata(metadataBuilder, applicationEntity);

        return new Application(
            applicationEntity.getUniqueId(),
            applicationEntity.getCreated(),
            applicationEntity.getUpdated(),
            new ExecutionEnvironment(
                applicationEntity.getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                applicationEntity.getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                applicationEntity.getSetupFile().isPresent() ? applicationEntity.getSetupFile().get().getFile() : null
            ),
            metadataBuilder.build()
        );
    }

    /**
     * Convert a cluster entity to a DTO for external exposure.
     *
     * @param clusterEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     */
    static Cluster toV4ClusterDto(final ClusterEntity clusterEntity) {
        final ClusterMetadata.Builder metadataBuilder = new ClusterMetadata.Builder(
            clusterEntity.getName(),
            clusterEntity.getUser(),
            clusterEntity.getStatus()
        )
            .withVersion(clusterEntity.getVersion())
            .withTags(clusterEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()));

        clusterEntity.getDescription().ifPresent(metadataBuilder::withDescription);
        setDtoMetadata(metadataBuilder, clusterEntity);

        return new Cluster(
            clusterEntity.getUniqueId(),
            clusterEntity.getCreated(),
            clusterEntity.getUpdated(),
            new ExecutionEnvironment(
                clusterEntity.getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                clusterEntity.getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                clusterEntity.getSetupFile().isPresent() ? clusterEntity.getSetupFile().get().getFile() : null
            ),
            metadataBuilder.build()
        );
    }

    /**
     * Convert a command entity to a DTO for external exposure.
     *
     * @param commandEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     */
    static Command toV4CommandDto(final CommandEntity commandEntity) {
        final CommandMetadata.Builder metadataBuilder = new CommandMetadata.Builder(
            commandEntity.getName(),
            commandEntity.getUser(),
            commandEntity.getStatus()
        )
            .withVersion(commandEntity.getVersion())
            .withTags(commandEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()));

        commandEntity.getDescription().ifPresent(metadataBuilder::withDescription);
        setDtoMetadata(metadataBuilder, commandEntity);

        return new Command(
            commandEntity.getUniqueId(),
            commandEntity.getCreated(),
            commandEntity.getUpdated(),
            new ExecutionEnvironment(
                commandEntity.getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                commandEntity.getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                commandEntity.getSetupFile().isPresent() ? commandEntity.getSetupFile().get().getFile() : null
            ),
            metadataBuilder.build(),
            Lists.newArrayList(StringUtils.split(commandEntity.getExecutable(), ' ')),
            commandEntity.getMemory().orElse(null)
        );
    }

    private static <B extends CommonMetadata.Builder, E extends BaseProjection> void setDtoMetadata(
        final B builder,
        final E entity
    ) {
        if (entity.getMetadata().isPresent()) {
            try {
                builder.withMetadata(entity.getMetadata().get());
            } catch (final GeniePreconditionException gpe) {
                // Since the DTO can't be constructed on input with invalid JSON this should never even happen
                log.error("Invalid JSON metadata. Should never happen.", gpe);
                builder.withMetadata((JsonNode) null);
            }
        }
    }
}
