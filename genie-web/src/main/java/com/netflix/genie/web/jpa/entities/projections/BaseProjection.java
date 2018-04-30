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
 * Projection for the common fields.
 *
 * @author tgianos
 * @since 3.3.0
 */
public interface BaseProjection extends UniqueIdProjection {

    /**
     * Get the version.
     *
     * @return The version of the resource (job, app, etc)
     */
    String getVersion();

    /**
     * Get the user who created the resource.
     *
     * @return The user who created the resource
     */
    String getUser();

    /**
     * Get the name of the resource.
     *
     * @return The name of the resource
     */
    String getName();

    /**
     * Get the description of this resource.
     *
     * @return The description which could be null so it's wrapped in Optional
     */
    Optional<String> getDescription();

    /**
     * Get the metadata of this entity which is unstructured JSON.
     *
     * @return Optional of the metadata json node represented as a string
     */
    Optional<String> getMetadata();
}
