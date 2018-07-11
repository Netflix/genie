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
package com.netflix.genie.web.jpa.services;

import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.services.TagPersistenceService;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Optional;
import java.util.Set;

/**
 * Extension of the {@link TagPersistenceService} to add APIs for entity retrieval in addition to DTO.
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface JpaTagPersistenceService extends TagPersistenceService {

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
