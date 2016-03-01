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

import com.netflix.genie.core.jpa.entities.JobExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.Set;

/**
 * Job repository.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Repository
public interface JpaJobExecutionRepository extends JpaRepository<JobExecutionEntity, String>, JpaSpecificationExecutor {

    /**
     * Get all the job executions which are on the given host with the given exit code.
     *
     * @param hostname The hostname to search for
     * @param exitCode The exit code to search for
     * @return All the job executions currently running on that host
     */
    Set<JobExecutionEntity> findByHostnameAndExitCode(final String hostname, final int exitCode);
}
