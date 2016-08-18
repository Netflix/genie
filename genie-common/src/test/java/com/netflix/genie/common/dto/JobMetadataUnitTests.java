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
 * Unit tests for the JobMetadata class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobMetadataUnitTests {

    /**
     * Test to make sure we can successfully build a JobMetadata class.
     *
     * @throws Exception on error
     */
    @Test
    public void canBuild() throws Exception {
        final String clientHost = UUID.randomUUID().toString();
        final String userAgent = UUID.randomUUID().toString();
        final int numAttachments = 38;
        final long totalSizeOfAttachments = 3809234L;
        final long stdOutSize = 80283L;
        final long stdErrSize = 8002343L;

        final JobMetadata metadata = new JobMetadata
            .Builder()
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments)
            .withStdOutSize(stdOutSize)
            .withStdErrSize(stdErrSize)
            .build();

        Assert.assertThat(metadata.getClientHost().orElseThrow(IllegalArgumentException::new), Matchers.is(clientHost));
        Assert.assertThat(metadata.getUserAgent().orElseThrow(IllegalArgumentException::new), Matchers.is(userAgent));
        Assert.assertThat(
            metadata.getNumAttachments().orElseThrow(IllegalArgumentException::new),
            Matchers.is(numAttachments)
        );
        Assert.assertThat(
            metadata.getTotalSizeOfAttachments().orElseThrow(IllegalArgumentException::new),
            Matchers.is(totalSizeOfAttachments)
        );
        Assert.assertThat(
            metadata.getStdOutSize().orElseThrow(IllegalArgumentException::new),
            Matchers.is(stdOutSize)
        );
        Assert.assertThat(
            metadata.getStdErrSize().orElseThrow(IllegalArgumentException::new),
            Matchers.is(stdErrSize)
        );
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

        final JobMetadata.Builder builder = new JobMetadata
            .Builder()
            .withId(UUID.randomUUID().toString())
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments);

        final JobMetadata one = builder.build();
        final JobMetadata two = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobMetadata three = builder.build();

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

        final JobMetadata.Builder builder = new JobMetadata
            .Builder()
            .withId(UUID.randomUUID().toString())
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments);

        final JobMetadata one = builder.build();
        final JobMetadata two = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobMetadata three = builder.build();

        Assert.assertEquals(one.hashCode(), two.hashCode());
        Assert.assertNotEquals(one.hashCode(), three.hashCode());
    }
}
