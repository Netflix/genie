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
package com.netflix.genie.core.util;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Class containing unit tests for Utils.java.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class UtilsUnitTests {

    /**
     * Tests the getFileName method functionality.
     *
     * @throws GenieException if there is any problem
     */
    @Test
    public void testGetFileNameFromPath() throws GenieException {
        final String testFilePath = "oo/bar/name";
        Assert.assertEquals("name", Utils.getFileNameFromPath(testFilePath));
    }

    /**
     * Test the getProcessId method for failure.
     *
     * @throws GenieException If there is a problem.
     * @throws IOException If there is a problem.
     */
    @Test(expected = GenieException.class)
    public void testGetProcessIdMethodFailure() throws GenieException, IOException {
        final Process process = Mockito.mock(Process.class);
        Utils.getProcessId(process);
    }

    /**
     * Test the getProcessId method for success.
     *
     * @throws GenieException If there is a problem.
     * @throws IOException If there is a problem.
     */
    @Test
    public void testGetProcessIdMethodSuccess() throws GenieException, IOException {
        final Process process = Runtime.getRuntime().exec("ls /dev/null");
        Utils.getProcessId(process);
    }
}
