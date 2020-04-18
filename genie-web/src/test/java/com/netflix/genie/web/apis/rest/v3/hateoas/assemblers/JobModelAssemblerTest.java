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

import com.netflix.genie.common.dto.Job;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.hateoas.EntityModel;

import java.util.UUID;

/**
 * Unit tests for the JobResourceAssembler.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobModelAssemblerTest {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();

    private Job job;
    private JobModelAssembler assembler;

    @BeforeEach
    void setup() {
        this.job = new Job.Builder(NAME, USER, VERSION).withId(ID).build();
        this.assembler = new JobModelAssembler();
    }

    @Test
    public void canConstruct() {
        Assertions.assertThat(this.assembler).isNotNull();
    }

    @Test
    public void canConvertToModel() {
        final EntityModel<Job> model = this.assembler.toModel(this.job);
        Assertions.assertThat(model.getLinks()).hasSize(9);
        Assertions.assertThat(model.getLink("self")).isPresent();
        Assertions.assertThat(model.getLink("output")).isPresent();
        Assertions.assertThat(model.getLink("request")).isPresent();
        Assertions.assertThat(model.getLink("execution")).isPresent();
        Assertions.assertThat(model.getLink("metadata")).isPresent();
        Assertions.assertThat(model.getLink("status")).isPresent();
        Assertions.assertThat(model.getLink("cluster")).isPresent();
        Assertions.assertThat(model.getLink("command")).isPresent();
        Assertions.assertThat(model.getLink("applications")).isPresent();
    }
}
