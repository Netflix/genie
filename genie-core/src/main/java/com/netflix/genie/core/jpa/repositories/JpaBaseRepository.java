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

import com.netflix.genie.core.jpa.entities.BaseEntity;
import org.springframework.data.repository.NoRepositoryBean;

import java.util.Optional;

/**
 * A common repository for inheritance of common methods for Entities extending BaseEntity.
 *
 * @param <E> The entity class to act on which must extend BaseEntity
 * @author tgianos
 * @since 3.3.0
 */
@NoRepositoryBean
public interface JpaBaseRepository<E extends BaseEntity> extends JpaIdRepository<E> {
    /**
     * Find an entity by its unique id.
     *
     * @param uniqueId The unique id to find an entity for
     * @return The entity found or empty Optional
     */
    Optional<E> findByUniqueId(final String uniqueId);

    // TODO: Make interfaces generic but be aware of https://jira.spring.io/browse/DATAJPA-1185

    /**
     * Find an entity by its unique id.
     *
     * @param uniqueId The unique id to find an entity for
     * @param type     The entity or projection type to return
     * @return The entity found or empty Optional
     */
    <T> Optional<T> findByUniqueId(final String uniqueId, final Class<T> type);

    /**
     * Find out whether an entity with the given unique id exists.
     *
     * @param uniqueId The unique id to check for existence
     * @return True if an entity with the unique id exists
     */
    boolean existsByUniqueId(final String uniqueId);
}
