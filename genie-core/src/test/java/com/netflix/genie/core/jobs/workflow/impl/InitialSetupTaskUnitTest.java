/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.jobs.workflow.impl;

import com.netflix.genie.test.categories.UnitTest;
import com.netflix.spectator.api.Registry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Tests for the InitialSetup task.
 *
 * @author mprimi
 * @since 3.1.0
 */
@Category(UnitTest.class)
public class InitialSetupTaskUnitTest {

    private Registry registry;
    private InitialSetupTask initialSetupTask;

    /**
     * Setup to run before each test.
     */
    @Before
    public void setUp() {
        this.registry = Mockito.mock(Registry.class);
        this.initialSetupTask = new InitialSetupTask(registry);
    }

    /**
     * Test helper method to serialize cluster/command tags.
     */
    @Test
    public void tagsToString() {

        Assert.assertEquals("", initialSetupTask.tagsToString(null));

        final String[][] inputs = new String[][] {
            {},
            {"some.tag:t"},
            {"foo", "bar"},
            {"bar", "foo"},
            {"foo", "bar", "tag,with,commas"},
            {"foo", "bar", "tag with spaces"},
            {"foo", "bar", "tag\nwith\nnewlines"},
            {"foo", "bar", "\"tag-with-double-quotes\""},
            {"foo", "bar", "\'tag-with-single-quotes\'"},
        };

        final String[] outputs = {
            "",
            "some.tag:t",
            "bar,foo",
            "bar,foo",
            "bar,foo,tag,with,commas", // Commas in tags are not escaped
            "bar,foo,tag with spaces",
            "bar,foo,tag\nwith\nnewlines",
            "\"tag-with-double-quotes\",bar,foo",
            "\'tag-with-single-quotes\',bar,foo",
        };

        Assert.assertEquals(inputs.length, outputs.length);
        for (int i = 0; i < inputs.length; i++) {
            final String expectedOutputString = outputs[i];
            final HashSet<String> tags = new HashSet<>(Arrays.asList(inputs[i]));
            Assert.assertEquals(expectedOutputString, this.initialSetupTask.tagsToString(tags));
        }
    }
}
