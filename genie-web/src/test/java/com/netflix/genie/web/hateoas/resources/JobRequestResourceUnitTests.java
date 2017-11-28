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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for the JobRequestResource class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobRequestResourceUnitTests {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final String COMMAND_ARGS = UUID.randomUUID().toString();
    private static final List<ClusterCriteria> CLUSTER_CRITERIAS = Lists.newArrayList(
        new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())),
        new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())),
        new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
    );
    private static final Set<String> COMMAND_CRITERIA = Sets.newHashSet(
        UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()
    );

    private JobRequest jobRequest;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobRequest = new JobRequest
            .Builder(NAME, USER, VERSION, CLUSTER_CRITERIAS, COMMAND_CRITERIA)
            .withId(ID)
            .build();
    }

    /**
     * Make sure we can build the resource.
     */
    @Test
    public void canBuildResource() {
        Assert.assertNotNull(new JobRequestResource(this.jobRequest));
    }
}
