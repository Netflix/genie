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
package com.netflix.genie.common.dto.search;

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Date;
import java.util.UUID;

/**
 * Tests for the JobSearchResult DTO.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobSearchResultUnitTests {

    /**
     * Make sure constructor works.
     */
    @Test
    public void canConstruct() {
        final String id = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        final JobStatus status = JobStatus.FAILED;
        final Date started = new Date();
        final Date finished = new Date();
        final String clusterName = UUID.randomUUID().toString();
        final String commandName = UUID.randomUUID().toString();
        final JobSearchResult searchResult
            = new JobSearchResult(id, name, status, started, finished, clusterName, commandName);

        Assert.assertThat(searchResult.getId(), Matchers.is(id));
        Assert.assertThat(searchResult.getName(), Matchers.is(name));
        Assert.assertThat(searchResult.getStatus(), Matchers.is(status));
        Assert.assertThat(searchResult.getStarted(), Matchers.is(started));
        Assert.assertThat(searchResult.getFinished(), Matchers.is(finished));
        Assert.assertThat(searchResult.getClusterName(), Matchers.is(clusterName));
        Assert.assertThat(searchResult.getCommandName(), Matchers.is(commandName));

        final JobSearchResult searchResult2 = new JobSearchResult(id, name, status, null, null, null, null);

        Assert.assertThat(searchResult2.getId(), Matchers.is(id));
        Assert.assertThat(searchResult2.getName(), Matchers.is(name));
        Assert.assertThat(searchResult2.getStatus(), Matchers.is(status));
        Assert.assertNull(searchResult2.getStarted());
        Assert.assertNull(searchResult2.getFinished());
        Assert.assertNull(searchResult2.getClusterName());
        Assert.assertNull(searchResult2.getCommandName());
    }
}
