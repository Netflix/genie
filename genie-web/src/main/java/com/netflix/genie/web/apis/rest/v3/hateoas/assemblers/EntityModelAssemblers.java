/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.apis.rest.v3.hateoas.assemblers;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A simple container DTO for passing all known resource assemblers around.
 *
 * @author tgianos
 * @since 4.0.0
 */
@RequiredArgsConstructor
@Getter
public class EntityModelAssemblers {
    private final ApplicationModelAssembler applicationModelAssembler;
    private final ClusterModelAssembler clusterModelAssembler;
    private final CommandModelAssembler commandModelAssembler;
    private final JobExecutionModelAssembler jobExecutionModelAssembler;
    private final JobMetadataModelAssembler jobMetadataModelAssembler;
    private final JobRequestModelAssembler jobRequestModelAssembler;
    private final JobModelAssembler jobModelAssembler;
    private final JobSearchResultModelAssembler jobSearchResultModelAssembler;
    private final RootModelAssembler rootModelAssembler;
}
