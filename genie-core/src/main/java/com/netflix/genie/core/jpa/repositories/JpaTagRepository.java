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
package com.netflix.genie.core.jpa.repositories;

import com.netflix.genie.core.jpa.entities.TagEntity;

import java.util.Optional;
import java.util.Set;

/**
 * Repository for tags.
 *
 * @author tgianos
 * @since 3.3.0
 */
public interface JpaTagRepository extends BaseRepository<TagEntity> {

    /**
     * Find a tag by its unique tag value.
     *
     * @param tag The tag value to search for
     * @return An Optional of a TagEntity
     */
    Optional<TagEntity> findByTag(final String tag);

    /**
     * Find out whether a tag entity with the given tag value exists.
     *
     * @param tag The tag value to check for
     * @return True if the tag exists
     */
    boolean existsByTag(final String tag);

    /**
     * Find tag entities where the tag value is in the given set of tags.
     *
     * @param tags The tags to find entities for
     * @return The tag entites
     */
    Set<TagEntity> findByTagIn(final Set<String> tags);
}
