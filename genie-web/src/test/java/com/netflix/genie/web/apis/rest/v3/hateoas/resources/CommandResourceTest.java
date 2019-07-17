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
package com.netflix.genie.web.apis.rest.v3.hateoas.resources;

import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

/**
 * Unit tests for the CommandResource class.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class CommandResourceTest {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final String EXECUTABLE = UUID.randomUUID().toString();
    private static final long CHECK_DELAY = 1380L;

    private Command command;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.command = new Command
            .Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY)
            .withId(ID)
            .build();
    }

    /**
     * Make sure we can build the resource.
     */
    @Test
    public void canBuildResource() {
        Assert.assertNotNull(new CommandResource(this.command));
    }
}
