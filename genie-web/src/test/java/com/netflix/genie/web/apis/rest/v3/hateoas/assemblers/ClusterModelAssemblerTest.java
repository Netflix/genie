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

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.hateoas.EntityModel;

import java.util.UUID;

/**
 * Unit tests for the ClusterResourceAssembler.
 *
 * @author tgianos
 * @since 3.0.0
 */
class ClusterModelAssemblerTest {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();

    private Cluster cluster;
    private ClusterModelAssembler assembler;

    @BeforeEach
    void setup() {
        this.cluster = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP).withId(ID).build();
        this.assembler = new ClusterModelAssembler();
    }

    @Test
    void canConstruct() {
        Assertions.assertThat(this.assembler).isNotNull();
    }

    @Test
    void canConvertToModel() {
        final EntityModel<Cluster> model = this.assembler.toModel(this.cluster);
        Assertions.assertThat(model.getLinks()).hasSize(2);
        Assertions.assertThat(model.getLink("self")).isPresent();
        Assertions.assertThat(model.getLink("commands")).isPresent();
    }
}
