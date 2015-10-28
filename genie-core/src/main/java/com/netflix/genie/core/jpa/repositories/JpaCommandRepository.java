/*
 * Copyright 2015 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */
package com.netflix.genie.core.jpa.repositories;

import com.netflix.genie.core.jpa.entities.CommandEntity;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

/**
 * Command repository.
 *
 * @author tgianos
 */
public interface JpaCommandRepository extends JpaRepository<CommandEntity, String>, JpaSpecificationExecutor {

    /**
     * Find commands by name.
     *
     * @param name The name of the command to find
     * @return The commands matching the name.
     */
    List<CommandEntity> findByName(final String name);
}
