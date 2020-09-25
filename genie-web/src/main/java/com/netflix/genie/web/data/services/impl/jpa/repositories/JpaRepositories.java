/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.data.services.impl.jpa.repositories;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Container class for encapsulating all the various JPA Repositories in Genie to ease dependency patterns.
 *
 * @author tgianos
 * @since 4.0.0
 */
@AllArgsConstructor
@Getter
public class JpaRepositories {
    private final JpaApplicationRepository applicationRepository;
    private final JpaClusterRepository clusterRepository;
    private final JpaCommandRepository commandRepository;
    private final JpaCriterionRepository criterionRepository;
    private final JpaFileRepository fileRepository;
    private final JpaJobRepository jobRepository;
    private final JpaTagRepository tagRepository;
}
