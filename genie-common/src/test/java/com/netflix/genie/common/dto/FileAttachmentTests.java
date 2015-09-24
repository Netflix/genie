/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;

/**
 * Tests for the FileAttachment class.
 *
 * @author tgianos
 */
public class FileAttachmentTests {

    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private static final String NAME = "someQuery.q";
    private static final String QUERY = "select * from myTable";
    private static final byte[] DATA = QUERY.getBytes(UTF8_CHARSET);

    private FileAttachment attachment;

    /**
     * Setup the attachment used for all the tests.
     */
    @Before
    public void setup() {
        this.attachment = new FileAttachment(NAME, DATA);
    }

    /**
     * Test to make sure the constructor stores the data properly.
     */
    @Test
    public void testConstructor() {
        Assert.assertThat(this.attachment.getName(), Matchers.is(NAME));
        Assert.assertThat(new String(this.attachment.getData(), UTF8_CHARSET), Matchers.is(QUERY));
    }

    /**
     * Test to make sure the data can't be modified externally.
     */
    @Test
    public void testCanNotModifyContents() {
        final byte[] data = this.attachment.getData();
        data[0] = '0';
        Assert.assertThat(new String(data, UTF8_CHARSET), Matchers.not(QUERY));
        Assert.assertThat(new String(this.attachment.getData(), UTF8_CHARSET), Matchers.is(QUERY));
    }
}
