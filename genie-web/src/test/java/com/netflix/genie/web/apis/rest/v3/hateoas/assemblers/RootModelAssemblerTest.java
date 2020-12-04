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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.hateoas.EntityModel;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for the {@link RootModelAssembler}.
 *
 * @author tgianos
 * @since 3.0.0
 */
class RootModelAssemblerTest {

    private RootModelAssembler assembler;

    @BeforeEach
    void setup() {
        this.assembler = new RootModelAssembler();
    }

    @Test
    void canConstruct() {
        Assertions.assertThat(this.assembler).isNotNull();
    }

    @Test
    void canConvertToResource() {
        final Map<String, String> metadata = new HashMap<>();
        metadata.put("description", "blah");
        final EntityModel<Map<String, String>> model = this.assembler.toModel(metadata);
        Assertions.assertThat(model.getLinks()).hasSize(5);
        Assertions.assertThat(model.getContent()).isNotNull();
        Assertions.assertThat(model.getLink("self")).isNotNull();
        Assertions.assertThat(model.getLink("applications")).isNotNull();
        Assertions.assertThat(model.getLink("commands")).isNotNull();
        Assertions.assertThat(model.getLink("clusters")).isNotNull();
        Assertions.assertThat(model.getLink("jobs")).isNotNull();
    }
}
