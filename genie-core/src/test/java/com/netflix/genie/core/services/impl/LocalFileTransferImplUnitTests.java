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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.test.categories.UnitTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.Executor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class contains unit tests for the class LocalFileTransferImpl.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
@Category(UnitTest.class)
public class LocalFileTransferImplUnitTests {

    private static final String COPY_COMMAND = "cp";
    private static final String SOURCE_FILE = "source";
    private static final String DESTINATION_FILE = "dest";

    private Executor executor;
    private LocalFileTransferImpl localFileTransfer;
    /**
     * Setup the tests.
     *
     * @throws GenieException If there is a problem.
     */
    @Before
    public void setup() throws GenieException {
        executor = Mockito.mock(Executor.class);
        localFileTransfer = new LocalFileTransferImpl(this.executor);
    }

    /**
     * Test the isValid method for valid file prefix file://.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testisValidWithCorrectFilePrefix() throws GenieException {
        Assert.assertEquals(localFileTransfer.isValid("file://filepath"), true);
    }

    /**
     * Test the isValid method for invalid file prefix not starting with file://.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testisValidWithInvalidFilePrefix() throws GenieException {
        Assert.assertEquals(localFileTransfer.isValid("filepath"), false);
    }

    /**
     * Test the getFile method.
     *
     * @throws GenieException If there is any problem
     * @throws IOException If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testGetFileMethod() throws GenieException, IOException {
        final ArgumentCaptor<CommandLine> argument = ArgumentCaptor.forClass(CommandLine.class);
        Mockito.when(this.executor.execute(Mockito.any())).thenThrow(IOException.class);

        this.localFileTransfer.getFile(SOURCE_FILE, DESTINATION_FILE);
        Mockito.verify(this.executor).execute(argument.capture());

        final List<String> expectedCommandLine = new ArrayList<>();
        expectedCommandLine.add(COPY_COMMAND);
        expectedCommandLine.add(SOURCE_FILE);
        expectedCommandLine.add(DESTINATION_FILE);

        Assert.assertArrayEquals(expectedCommandLine.toArray(), argument.getValue().getArguments());
    }

    /**
     * Test the putFile method.
     *
     * @throws GenieException If there is any problem
     * @throws IOException If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testPutFileMethod() throws GenieException, IOException {
        final ArgumentCaptor<CommandLine> argument = ArgumentCaptor.forClass(CommandLine.class);
        Mockito.when(this.executor.execute(Mockito.any())).thenThrow(IOException.class);

        this.localFileTransfer.putFile(SOURCE_FILE, DESTINATION_FILE);
        Mockito.verify(this.executor).execute(argument.capture());

        final List<String> expectedCommandLine = new ArrayList<>();
        expectedCommandLine.add(COPY_COMMAND);
        expectedCommandLine.add(SOURCE_FILE);
        expectedCommandLine.add(DESTINATION_FILE);

        Assert.assertArrayEquals(expectedCommandLine.toArray(), argument.getValue().getArguments());
    }
}
