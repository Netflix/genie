/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.util;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.Executor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Unit Tests for {@link UNIXUtils} class.
 * (this test class was once named JobKickoffTaskTest, but was only testing methods that now belong in UNIXUserHelper).
 *
 * @author mprimi
 * @since 4.0.0
 */
public class UNIXUtilsTest {

    private Executor executor;

    /**
     * Set up the tests.
     */
    @Before
    public void setUp() {
        this.executor = Mockito.mock(Executor.class);
    }

    /**
     * Test the change ownership method for success.
     *
     * @throws IOException If there is any problem.
     */
    @Test
    public void testChangeOwnershipOfDirectoryMethodSuccess() throws IOException {
        final String user = "user";
        final String dir = "dir";
        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        final List<String> command = Arrays.asList("sudo", "chown", "-R", user, dir);

        UNIXUtils.changeOwnershipOfDirectory(dir, user, executor);
        Mockito.verify(this.executor).execute(argumentCaptor.capture());
        Assert.assertArrayEquals(command.toArray(), argumentCaptor.getValue().toStrings());
    }

    /**
     * Test the change ownership method for success.
     *
     * @throws IOException If there is any problem.
     */
    @Test(expected = IOException.class)
    public void testChangeOwnershipOfDirectoryMethodFailure() throws IOException {
        final String user = "user";
        final String dir = "dir";

        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenThrow(new IOException());
        UNIXUtils.changeOwnershipOfDirectory(dir, user, executor);
    }

    /**
     * Test the create user method for user already exists.
     *
     * @throws IOException If there is any problem.
     */
    @Test
    public void testCreateUserMethodSuccessAlreadyExists() throws IOException {
        final String user = "user";
        final String group = "group";
        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        final List<String> command = Arrays.asList("id", "-u", user);

        UNIXUtils.createUser(user, group, executor);
        Mockito.verify(this.executor).execute(argumentCaptor.capture());
        Assert.assertArrayEquals(command.toArray(), argumentCaptor.getValue().toStrings());
    }

    /**
     * Test the create user method for user already exists.
     *
     * @throws IOException If there is any problem.
     */
    @Test
    public void testCreateUserMethodSuccessDoesNotExist1() throws IOException {
        final String user = "user";
        final String group = "group";

        final CommandLine idCheckCommandLine = new CommandLine("id");
        idCheckCommandLine.addArgument("-u");
        idCheckCommandLine.addArgument(user);

        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenThrow(new IOException());

        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        final List<String> command = Arrays.asList("sudo", "useradd", user, "-G", group, "-M");

        try {
            UNIXUtils.createUser(user, group, executor);
        } catch (IOException ge) {
            // ignore
        }

        Mockito.verify(this.executor, Mockito.times(3)).execute(argumentCaptor.capture());
        Assert.assertArrayEquals(command.toArray(), argumentCaptor.getAllValues().get(2).toStrings());
    }

    /**
     * Test the create user method for user already exists but swallow genie exception.
     *
     * @throws IOException If there is any problem.
     */
    @Test(expected = IOException.class)
    public void testCreateUserMethodSuccessDoesNotExist2() throws IOException {
        final String user = "user";
        final String group = "group";

        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenThrow(new IOException());
        UNIXUtils.createUser(user, group, executor);
    }
}
