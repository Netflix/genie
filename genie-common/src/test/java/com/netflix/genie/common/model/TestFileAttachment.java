/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.common.model;

import com.netflix.genie.common.exceptions.GenieException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Tests for the FileAttachment class.
 *
 * @author tgianos
 */
public class TestFileAttachment {

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
        this.attachment = new FileAttachment();
    }

    /**
     * Test the setter and getter for name.
     *
     * @throws GenieException
     */
    @Test
    public void testSetGetName() throws GenieException {
        Assert.assertNull(this.attachment.getName());
        this.attachment.setName(NAME);
        Assert.assertEquals(NAME, this.attachment.getName());
    }

    /**
     * Test setting a bad name.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testSetNameNull() throws GenieException {
        this.attachment.setName(null);
    }

    /**
     * Test setting a bad name.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testSetNameEmpty() throws GenieException {
        this.attachment.setName("");
    }

    /**
     * Test setting a bad name.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testSetNameBlank() throws GenieException {
        this.attachment.setName("   ");
    }

    /**
     * Test the setter and getter for data.
     *
     * @throws GenieException
     */
    @Test
    public void testSetGetData() throws GenieException {
        Assert.assertNull(this.attachment.getData());
        this.attachment.setData(DATA);
        Assert.assertTrue(Arrays.equals(DATA, attachment.getData()));
    }

    /**
     * Test the setting bad data.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testSetDataNull() throws GenieException {
        this.attachment.setData(null);
    }

    /**
     * Test the setting bad data.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testSetDataEmpty() throws GenieException {
        this.attachment.setData(new byte[0]);
    }
}
