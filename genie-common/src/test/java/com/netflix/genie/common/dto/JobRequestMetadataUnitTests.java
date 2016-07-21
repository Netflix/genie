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
package com.netflix.genie.common.dto;

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

/**
 * Unit tests for the JobRequestMetadata class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobRequestMetadataUnitTests {

    /**
     * Test to make sure we can successfully build a JobRequestMetadata class.
     */
    @Test
    public void canBuild() {
        final String clientHost = UUID.randomUUID().toString();
        final String userAgent = UUID.randomUUID().toString();
        final int numAttachments = 38;
        final long totalSizeOfAttachments = 3809234L;

        final JobRequestMetadata metadata = new JobRequestMetadata
            .Builder()
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments)
            .build();

        Assert.assertThat(metadata.getClientHost(), Matchers.is(clientHost));
        Assert.assertThat(metadata.getUserAgent(), Matchers.is(userAgent));
        Assert.assertThat(metadata.getNumAttachments(), Matchers.is(numAttachments));
        Assert.assertThat(metadata.getTotalSizeOfAttachments(), Matchers.is(totalSizeOfAttachments));
    }

    /**
     * Test to make sure we can successfully find equality.
     */
    @Test
    public void canFindEquality() {
        final String clientHost = UUID.randomUUID().toString();
        final String userAgent = UUID.randomUUID().toString();
        final int numAttachments = 38;
        final long totalSizeOfAttachments = 3809234L;

        final JobRequestMetadata.Builder builder = new JobRequestMetadata
            .Builder()
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments);

        final JobRequestMetadata one = builder.build();
        final JobRequestMetadata two = builder.build();
        builder.withClientHost(UUID.randomUUID().toString());
        final JobRequestMetadata three = builder.build();

        Assert.assertTrue(one.equals(two));
        Assert.assertFalse(one.equals(new Object()));
        Assert.assertFalse(one.equals(three));
    }

    /**
     * Test to make sure we can successfully find equality.
     */
    @Test
    public void canUseHashCode() {
        final String clientHost = UUID.randomUUID().toString();
        final String userAgent = UUID.randomUUID().toString();
        final int numAttachments = 38;
        final long totalSizeOfAttachments = 3809234L;

        final JobRequestMetadata.Builder builder = new JobRequestMetadata
            .Builder()
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments);

        final JobRequestMetadata one = builder.build();
        final JobRequestMetadata two = builder.build();
        builder.withClientHost(UUID.randomUUID().toString());
        final JobRequestMetadata three = builder.build();

        Assert.assertEquals(one.hashCode(), two.hashCode());
        Assert.assertNotEquals(one.hashCode(), three.hashCode());
    }
}
