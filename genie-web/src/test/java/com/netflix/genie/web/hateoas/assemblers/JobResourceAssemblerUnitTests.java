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
package com.netflix.genie.web.hateoas.assemblers;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.hateoas.resources.JobResource;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

/**
 * Unit tests for the JobResourceAssembler.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobResourceAssemblerUnitTests {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final String COMMAND_ARGS = UUID.randomUUID().toString();

    private Job job;
    private JobResourceAssembler assembler;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.job = new Job.Builder(NAME, USER, VERSION, COMMAND_ARGS).withId(ID).build();
        this.assembler = new JobResourceAssembler();
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
        final JobResource resource = this.assembler.toResource(this.job);
        Assert.assertThat(resource.getLinks().size(), Matchers.is(1));
        Assert.assertNotNull(resource.getLink("self"));
    }
}
