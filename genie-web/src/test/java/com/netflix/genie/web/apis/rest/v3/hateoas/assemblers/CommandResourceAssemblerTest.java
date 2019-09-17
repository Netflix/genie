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
import com.netflix.genie.web.apis.rest.v3.hateoas.resources.CommandResource;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

/**
 * Unit tests for the CommandResourceAssembler.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class CommandResourceAssemblerTest {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final List<String> EXECUTABLE_AND_ARGS = Lists.newArrayList(UUID.randomUUID().toString());
    private static final long CHECK_DELAY = 1000L;

    private Command command;
    private CommandResourceAssembler assembler;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.command = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
            .withId(ID)
            .build();
        this.assembler = new CommandResourceAssembler();
    }

    /**
     * Make sure we can construct the assembler.
     */
    @Test
    public void canConstruct() {
        Assert.assertNotNull(this.assembler);
    }

    /**
     * Make sure we can convert the DTO to a resource with links.
     */
    @Test
    @Ignore
    public void canConvertToResource() {
        final CommandResource resource = this.assembler.toResource(this.command);
        Assert.assertThat(resource.getLinks().size(), Matchers.is(2));
        Assert.assertNotNull(resource.getLink("self"));
        Assert.assertNotNull(resource.getLink("clusters"));
    }
}
