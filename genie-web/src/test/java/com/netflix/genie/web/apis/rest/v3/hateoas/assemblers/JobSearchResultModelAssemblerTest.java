/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.hateoas.EntityModel;

import java.time.Instant;
import java.util.UUID;

/**
 * Unit tests for the {@link JobSearchResultModelAssembler}.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobSearchResultModelAssemblerTest {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final Instant STARTED = Instant.now();
    private static final Instant FINISHED = Instant.now();
    private static final String CLUSTER_NAME = UUID.randomUUID().toString();
    private static final String COMMAND_NAME = UUID.randomUUID().toString();

    private JobSearchResult jobSearchResult;
    private JobSearchResultModelAssembler assembler;

    @BeforeEach
    void setup() {
        this.jobSearchResult
            = new JobSearchResult(ID, NAME, USER, JobStatus.SUCCEEDED, STARTED, FINISHED, CLUSTER_NAME, COMMAND_NAME);
        this.assembler = new JobSearchResultModelAssembler();
    }

    @Test
    void canConstruct() {
        Assertions.assertThat(this.assembler).isNotNull();
    }

    @Test
    void canConvertToResource() {
        final EntityModel<JobSearchResult> model = this.assembler.toModel(this.jobSearchResult);
        Assertions.assertThat(model.getLinks()).hasSize(1);
        Assertions.assertThat(model.getLink("self")).isNotNull();
    }
}
