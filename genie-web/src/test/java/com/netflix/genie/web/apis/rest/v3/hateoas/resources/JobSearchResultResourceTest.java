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
package com.netflix.genie.web.hateoas.resources;

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.UUID;

/**
 * Unit tests for the JobResource class.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class JobSearchResultResourceTest {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final Instant STARTED = Instant.now();

    private JobSearchResult jobSearchResult;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobSearchResult = new JobSearchResult(ID, NAME, USER, JobStatus.RUNNING, STARTED, null, null, null);
    }

    /**
     * Make sure we can build the resource.
     */
    @Test
    public void canBuildResource() {
        Assert.assertNotNull(new JobSearchResultResource(this.jobSearchResult));
    }
}
