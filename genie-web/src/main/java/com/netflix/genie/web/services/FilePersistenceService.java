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

import com.netflix.genie.web.jpa.entities.FileEntity;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * API definition for manipulating file references within Genie.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Validated
public interface FilePersistenceService {

    /**
     * Attempt to create a file reference in the system if it doesn't already exist.
     *
     * @param file the file to create. Not blank.
     */
    void createFileIfNotExists(@NotBlank(message = "File path cannot be blank") final String file);

    /**
     * Delete all files from the database that aren't referenced which were created before the supplied created
     * threshold.
     *
     * @param createdThreshold The instant in time where files created before this time that aren't referenced
     *                         will be deleted. Inclusive
     * @return The number of files deleted
     */
    long deleteUnusedFiles(@NotNull final Instant createdThreshold);

    /**
     * Get a reference to the file entity for the given file path string.
     *
     * @param file The path to search for
     * @return The entity wrapped in an {@link Optional} or {@link Optional#empty()}
     */
    Optional<FileEntity> getFile(@NotBlank(message = "File path cannot be blank") final String file);

    /**
     * Get all the file entity references that match the input set of file strings.
     *
     * @param files the file strings to search for
     * @return The set of files entities found. Note: if a file in {@code files} doesn't exist in database the set
     * returned will consist of all other discovered files. E.G. {@code files} is ("a", "b", "c") and only "b"
     * and "c" are in the database the returned set will contain "b" and "c". The method won't fail.
     */
    Set<FileEntity> getFiles(@NotNull final Set<String> files);
}
