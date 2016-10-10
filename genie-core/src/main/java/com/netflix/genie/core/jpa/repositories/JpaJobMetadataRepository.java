/*
 * Copyright 2016 Netflix, Inc.
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

import com.netflix.genie.core.jpa.entities.JobMetadataEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Job Metadata repository.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Repository
public interface JpaJobMetadataRepository extends JpaRepository<JobMetadataEntity, String>, JpaSpecificationExecutor {
    /**
     * Deletes all job metadatas for the given ids.
     * @param ids list of ids for which the job requests should be deleted
     * @return no. of metadatas deleted
     */
    Long deleteByIdIn(@NotNull final List<String> ids);
}
