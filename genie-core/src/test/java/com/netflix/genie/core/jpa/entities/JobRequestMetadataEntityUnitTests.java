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
package com.netflix.genie.core.jpa.entities;

import com.netflix.genie.common.dto.JobRequestMetadata;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.UUID;

/**
 * Unit tests for the JobRequestEntity class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobRequestMetadataEntityUnitTests {

    private JobRequestMetadataEntity entity;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.entity = new JobRequestMetadataEntity();
    }

    /**
     * Make sure can successfully construct a JobRequestEntity.
     *
     * @throws GenieException on error
     */
    @Test
    public void canConstruct() throws GenieException {
        Assert.assertThat(this.entity.getId(), Matchers.nullValue());
        Assert.assertThat(this.entity.getCreated(), Matchers.notNullValue());
        Assert.assertThat(this.entity.getUpdated(), Matchers.notNullValue());
        Assert.assertThat(this.entity.getClientHost(), Matchers.nullValue());
        Assert.assertThat(this.entity.getUserAgent(), Matchers.nullValue());
        Assert.assertThat(this.entity.getNumAttachments(), Matchers.is(0));
        Assert.assertThat(this.entity.getTotalSizeOfAttachments(), Matchers.is(0L));
    }

    /**
     * Make sure can set the client host name the request came from.
     */
    @Test
    public void canSetClientHost() {
        final String clientHost = UUID.randomUUID().toString();
        this.entity.setClientHost(clientHost);
        Assert.assertThat(this.entity.getClientHost(), Matchers.is(clientHost));
    }

    /**
     * Make sure we can set and get the user agent string.
     */
    @Test
    public void canSetUserAgent() {
        Assert.assertThat(this.entity.getUserAgent(), Matchers.nullValue());
        final String userAgent = UUID.randomUUID().toString();
        this.entity.setUserAgent(userAgent);
        Assert.assertThat(this.entity.getUserAgent(), Matchers.is(userAgent));
    }

    /**
     * Make sure we can set and get the attachments.
     */
    @Test
    public void canSetNumAttachments() {
        Assert.assertThat(this.entity.getNumAttachments(), Matchers.is(0));
        final int numAttachments = 380208;
        this.entity.setNumAttachments(numAttachments);
        Assert.assertThat(this.entity.getNumAttachments(), Matchers.is(numAttachments));
    }

    /**
     * Make sure we can set and get the attachments.
     */
    @Test
    public void canSetTotalSizeOfAttachments() {
        Assert.assertThat(this.entity.getTotalSizeOfAttachments(), Matchers.is(0L));
        final long totalSizeOfAttachments = 90832432L;
        this.entity.setTotalSizeOfAttachments(totalSizeOfAttachments);
        Assert.assertThat(this.entity.getTotalSizeOfAttachments(), Matchers.is(totalSizeOfAttachments));
    }

    /**
     * Make sure we can set and get the job request entity.
     */
    @Test
    public void canSetJobRequest() {
        Assert.assertThat(this.entity.getRequest(), Matchers.nullValue());
        final JobRequestEntity requestEntity = Mockito.mock(JobRequestEntity.class);
        this.entity.setRequest(requestEntity);
        Assert.assertThat(this.entity.getRequest(), Matchers.is(requestEntity));
    }

    /**
     * Test to make sure can get a valid DTO from the job request entity.
     *
     * @throws GenieException on error
     */
    @Test
    public void canGetDTO() throws GenieException {
        final JobRequestMetadataEntity requestEntity = new JobRequestMetadataEntity();
        final String id = UUID.randomUUID().toString();
        requestEntity.setId(id);
        final String clientHost = UUID.randomUUID().toString();
        requestEntity.setClientHost(clientHost);
        final String userAgent = UUID.randomUUID().toString();
        requestEntity.setUserAgent(userAgent);
        final int numAttachments = 3;
        requestEntity.setNumAttachments(numAttachments);
        final long totalSizeOfAttachments = 38023423L;
        requestEntity.setTotalSizeOfAttachments(totalSizeOfAttachments);

        final JobRequestMetadata metadata = requestEntity.getDTO();

        Assert.assertThat(metadata.getId(), Matchers.is(id));
        Assert.assertThat(metadata.getClientHost(), Matchers.is(clientHost));
        Assert.assertThat(metadata.getUserAgent(), Matchers.is(userAgent));
        Assert.assertThat(metadata.getNumAttachments(), Matchers.is(numAttachments));
        Assert.assertThat(metadata.getTotalSizeOfAttachments(), Matchers.is(totalSizeOfAttachments));
    }
}
