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

import com.netflix.genie.core.jpa.entities.FileEntity;

import java.util.Optional;

/**
 * Repository for file references.
 *
 * @author tgianos
 * @since 3.3.0
 */
public interface JpaFileRepository extends BaseRepository<FileEntity> {

    /**
     * Find a file by its unique file value.
     *
     * @param file The file value to search for
     * @return An Optional of a FileEntity
     */
    Optional<FileEntity> findByFile(final String file);

    /**
     * Find out whether a file entity with the given file value exists.
     *
     * @param file The file value to check for
     * @return True if the file exists
     */
    boolean existsByFile(final String file);
}
