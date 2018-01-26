/*
 *
 *  Copyright 2018 Netflix, Inc.
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

import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

/**
 * Unit tests for the JobExecutionResource class.
 *
 * @author tgianos
 * @since 3.3.5
 */
@Category(UnitTest.class)
public class JobMetadataResourceUnitTests {

    private static final String CLIENT_HOST = UUID.randomUUID().toString();
    private static final String USER_AGENT = UUID.randomUUID().toString();

    private JobMetadata jobMetadata;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobMetadata = new JobMetadata.Builder()
            .withId(UUID.randomUUID().toString())
            .withClientHost(CLIENT_HOST)
            .withUserAgent(USER_AGENT)
            .build();
    }

    /**
     * Make sure we can build the resource.
     */
    @Test
    public void canBuildResource() {
        Assert.assertNotNull(new JobMetadataResource(this.jobMetadata));
    }
}
