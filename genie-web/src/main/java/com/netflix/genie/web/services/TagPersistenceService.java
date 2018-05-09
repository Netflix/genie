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
package com.netflix.genie.web.services;

import com.netflix.genie.web.jpa.entities.TagEntity;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * API definition for manipulating tag references within Genie.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Validated
public interface TagPersistenceService {

    /**
     * Attempt to create a tag in the system if it doesn't already exist.
     *
     * @param tag the tag to create. Not blank.
     */
    void createTagIfNotExists(@NotBlank(message = "Tag cannot be blank") final String tag);

    /**
     * Delete all tags from the database that aren't referenced which were created before the supplied created
     * threshold.
     *
     * @param createdThreshold The instant in time where tags created before this time that aren't referenced
     *                         will be deleted. Inclusive
     * @return The number of tags deleted
     */
    long deleteUnusedTags(@NotNull final Instant createdThreshold);

    /**
     * Get a tag entity reference for the given tag string.
     *
     * @param tag The tag to get
     * @return The tag entity wrapped in an {@link Optional} or {@link Optional#empty()}
     */
    Optional<TagEntity> getTag(@NotBlank(message = "Tag string to find can't be blank") final String tag);

    /**
     * Get all the tag entity references that match the input set of tag strings.
     *
     * @param tags the tag strings to search for
     * @return The set of tag entities found. Note: if a tag in {@code tags} doesn't exist in database the set returned
     * will consist of all other discovered tags. E.G. {@code tags} is ("a", "b", "c") and only "b" and "c" are
     * in the database the returned set will contain "b" and "c". The method won't fail.
     */
    Set<TagEntity> getTags(@NotNull final Set<String> tags);
}
