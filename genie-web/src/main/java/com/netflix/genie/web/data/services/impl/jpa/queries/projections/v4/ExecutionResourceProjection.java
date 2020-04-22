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
package com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4;

import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.web.data.services.impl.jpa.entities.FileEntity;

import java.util.Optional;
import java.util.Set;

/**
 * A projection for fields from an entity which are needed for an
 * {@link JobSpecification.ExecutionResource}.
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface ExecutionResourceProjection {
    /**
     * Get the unique identifier for this entity.
     *
     * @return The globally unique identifier of this entity
     */
    String getUniqueId();

    /**
     * Get all the configuration files for this entity.
     *
     * @return The set of configs
     */
    Set<FileEntity> getConfigs();

    /**
     * Get all the dependency files for this entity.
     *
     * @return The set of dependencies
     */
    Set<FileEntity> getDependencies();

    /**
     * Get the setup file for this entity.
     *
     * @return The setup file
     */
    Optional<FileEntity> getSetupFile();
}
