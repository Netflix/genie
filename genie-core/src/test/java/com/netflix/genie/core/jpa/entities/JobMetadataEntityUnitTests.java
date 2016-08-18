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

import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.UUID;

/**
 * Unit tests for the JobMetadataEntity class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobMetadataEntityUnitTests {

    private JobMetadataEntity entity;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.entity = new JobMetadataEntity();
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
        Assert.assertFalse(this.entity.getClientHost().isPresent());
        Assert.assertFalse(this.entity.getUserAgent().isPresent());
        Assert.assertFalse(this.entity.getNumAttachments().isPresent());
        Assert.assertFalse(this.entity.getTotalSizeOfAttachments().isPresent());
    }

    /**
     * Make sure can set the client host name the request came from.
     */
    @Test
    public void canSetClientHost() {
        final String clientHost = UUID.randomUUID().toString();
        this.entity.setClientHost(clientHost);
        Assert.assertThat(this.entity.getClientHost().orElseGet(RandomSuppliers.STRING), Matchers.is(clientHost));
    }

    /**
     * Make sure we can set and get the user agent string.
     */
    @Test
    public void canSetUserAgent() {
        Assert.assertFalse(this.entity.getUserAgent().isPresent());
        final String userAgent = UUID.randomUUID().toString();
        this.entity.setUserAgent(userAgent);
        Assert.assertThat(
            this.entity.getUserAgent().orElseGet(RandomSuppliers.STRING),
            Matchers.is(userAgent)
        );
    }

    /**
     * Make sure we can set and get the number of attachments.
     */
    @Test
    public void canSetNumAttachments() {
        Assert.assertFalse(this.entity.getNumAttachments().isPresent());
        final int numAttachments = 380208;
        this.entity.setNumAttachments(numAttachments);
        Assert.assertThat(this.entity.getNumAttachments().orElseGet(RandomSuppliers.INT), Matchers.is(numAttachments));
    }

    /**
     * Make sure we can set and get the total size of the attachments.
     */
    @Test
    public void canSetTotalSizeOfAttachments() {
        Assert.assertFalse(this.entity.getTotalSizeOfAttachments().isPresent());
        final long totalSizeOfAttachments = 90832432L;
        this.entity.setTotalSizeOfAttachments(totalSizeOfAttachments);
        Assert.assertThat(
            this.entity.getTotalSizeOfAttachments().orElseGet(RandomSuppliers.LONG),
            Matchers.is(totalSizeOfAttachments)
        );
    }

    /**
     * Make sure we can set and get the size of the std out file.
     */
    @Test
    public void canSetStdOutSize() {
        Assert.assertFalse(this.entity.getStdOutSize().isPresent());
        final long stdOutSize = 90334432L;
        this.entity.setStdOutSize(stdOutSize);
        Assert.assertThat(
            this.entity.getStdOutSize().orElseGet(RandomSuppliers.LONG),
            Matchers.is(stdOutSize)
        );
    }

    /**
     * Make sure we can set and get the size of the std err file.
     */
    @Test
    public void canSetStdErrSize() {
        Assert.assertFalse(this.entity.getStdErrSize().isPresent());
        final long stdErrSize = 9089932L;
        this.entity.setStdErrSize(stdErrSize);
        Assert.assertThat(
            this.entity.getStdErrSize().orElseGet(RandomSuppliers.LONG),
            Matchers.is(stdErrSize)
        );
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
        final JobMetadataEntity requestEntity = new JobMetadataEntity();
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
        final long stdOutSize = 8088234L;
        requestEntity.setStdOutSize(stdOutSize);
        final long stdErrSize = 898088234L;
        requestEntity.setStdErrSize(stdErrSize);

        final JobMetadata metadata = requestEntity.getDTO();

        Assert.assertThat(metadata.getId().orElseGet(RandomSuppliers.STRING), Matchers.is(id));
        Assert.assertThat(metadata.getClientHost().orElseGet(RandomSuppliers.STRING), Matchers.is(clientHost));
        Assert.assertThat(metadata.getUserAgent().orElseGet(RandomSuppliers.STRING), Matchers.is(userAgent));
        Assert.assertThat(metadata.getNumAttachments().orElseGet(RandomSuppliers.INT), Matchers.is(numAttachments));
        Assert.assertThat(
            metadata.getTotalSizeOfAttachments().orElseGet(RandomSuppliers.LONG),
            Matchers.is(totalSizeOfAttachments)
        );
        Assert.assertThat(metadata.getStdOutSize().orElseGet(RandomSuppliers.LONG), Matchers.is(stdOutSize));
        Assert.assertThat(metadata.getStdErrSize().orElseGet(RandomSuppliers.LONG), Matchers.is(stdErrSize));
    }
}
