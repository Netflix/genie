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

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.web.hateoas.resources.JobSearchResultResource;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;
import java.util.UUID;

/**
 * Unit tests for the JobSearchResultResourceAssembler.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class JobSearchResultResourceAssemblerTest {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final Instant STARTED = Instant.now();
    private static final Instant FINISHED = Instant.now();
    private static final String CLUSTER_NAME = UUID.randomUUID().toString();
    private static final String COMMAND_NAME = UUID.randomUUID().toString();

    private JobSearchResult jobSearchResult;
    private JobSearchResultResourceAssembler assembler;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobSearchResult
            = new JobSearchResult(ID, NAME, USER, JobStatus.SUCCEEDED, STARTED, FINISHED, CLUSTER_NAME, COMMAND_NAME);
        this.assembler = new JobSearchResultResourceAssembler();
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
        final JobSearchResultResource resource = this.assembler.toResource(this.jobSearchResult);
        Assert.assertThat(resource.getLinks().size(), Matchers.is(1));
        Assert.assertNotNull(resource.getLink("self"));
    }
}
