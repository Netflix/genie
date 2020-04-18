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

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.hateoas.EntityModel;

import java.util.List;
import java.util.UUID;

/**
 * Unit tests for the CommandResourceAssembler.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class CommandModelAssemblerTest {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final List<String> EXECUTABLE_AND_ARGS = Lists.newArrayList(UUID.randomUUID().toString());
    private static final long CHECK_DELAY = 1000L;

    private Command command;
    private CommandModelAssembler assembler;

    @BeforeEach
    void setup() {
        this.command = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
            .withId(ID)
            .build();
        this.assembler = new CommandModelAssembler();
    }

    @Test
    void canConstruct() {
        Assertions.assertThat(this.assembler).isNotNull();
    }

    @Test
    void canConvertToResource() {
        final EntityModel<Command> model = this.assembler.toModel(this.command);
        Assertions.assertThat(model.getLinks()).hasSize(3);
        Assertions.assertThat(model.getLink("self")).isPresent();
        Assertions.assertThat(model.getLink("applications")).isPresent();
        Assertions.assertThat(model.getLink("clusters")).isPresent();
    }
}
