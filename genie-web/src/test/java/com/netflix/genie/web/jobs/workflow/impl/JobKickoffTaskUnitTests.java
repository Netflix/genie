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
package com.netflix.genie.web.jobs.workflow.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.test.categories.UnitTest;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import java.util.Arrays;
import java.util.List;

/**
 * Unit Tests for JobKickoffTask class.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(UnitTest.class)
@Slf4j
public class JobKickoffTaskUnitTests {

    private Executor executor;
    private JobKickoffTask jobKickoffTask;

    /**
     * Set up the tests.
     */
    @Before
    public void setUp() {
        this.executor = Mockito.mock(Executor.class);
        this.jobKickoffTask = new JobKickoffTask(
            false,
            false,
            this.executor,
            "localhost",
            new SimpleMeterRegistry()
        );
    }

    /**
     * Test the change ownership method for success.
     *
     * @throws IOException    If there is any problem.
     * @throws GenieException If there is any problem.
     */
    @Test
    public void testChangeOwnershipOfDirectoryMethodSuccess() throws IOException, GenieException {
        final String user = "user";
        final String dir = "dir";
        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        final List<String> command = Arrays.asList("sudo", "chown", "-R", user, dir);

        this.jobKickoffTask.changeOwnershipOfDirectory(
            dir,
            user
        );
        Mockito.verify(this.executor).execute(argumentCaptor.capture());
        Assert.assertArrayEquals(command.toArray(), argumentCaptor.getValue().toStrings());
    }

    /**
     * Test the change ownership method for success.
     *
     * @throws IOException    If there is any problem.
     * @throws GenieException If there is any problem.
     */
    @Test(expected = GenieServerException.class)
    public void testChangeOwnershipOfDirectoryMethodFailure() throws IOException, GenieException {
        final String user = "user";
        final String dir = "dir";

        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenThrow(new IOException());
        this.jobKickoffTask.changeOwnershipOfDirectory(dir, user);
    }

    /**
     * Test the create user method for user already exists.
     *
     * @throws IOException    If there is any problem.
     * @throws GenieException If there is any problem.
     */
    @Test
    public void testCreateUserMethodSuccessAlreadyExists() throws IOException, GenieException {
        final String user = "user";
        final String group = "group";
        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        final List<String> command = Arrays.asList("id", "-u", user);

        this.jobKickoffTask.createUser(user, group);
        Mockito.verify(this.executor).execute(argumentCaptor.capture());
        Assert.assertArrayEquals(command.toArray(), argumentCaptor.getValue().toStrings());
    }

    /**
     * Test the create user method for user already exists.
     *
     * @throws IOException    If there is any problem.
     * @throws GenieException If there is any problem.
     */
    @Test
    public void testCreateUserMethodSuccessDoesNotExist1() throws IOException, GenieException {
        final String user = "user";
        final String group = "group";

        final CommandLine idCheckCommandLine = new CommandLine("id");
        idCheckCommandLine.addArgument("-u");
        idCheckCommandLine.addArgument(user);

        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenThrow(new IOException());

        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        final List<String> command = Arrays.asList("sudo", "useradd", user, "-G", group, "-M");

        try {
            this.jobKickoffTask.createUser(user, group);
        } catch (GenieException ge) {
            log.debug("Ignoring exception to capture arguments.");
        }

        Mockito.verify(this.executor, Mockito.times(3)).execute(argumentCaptor.capture());
        Assert.assertArrayEquals(command.toArray(), argumentCaptor.getAllValues().get(2).toStrings());
    }

    /**
     * Test the create user method for user already exists but swallow genie exception.
     *
     * @throws IOException    If there is any problem.
     * @throws GenieException If there is any problem.
     */
    @Test(expected = GenieServerException.class)
    public void testCreateUserMethodSuccessDoesNotExist2() throws IOException, GenieException {
        final String user = "user";
        final String group = "group";

        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenThrow(new IOException());
        this.jobKickoffTask.createUser(
            user,
            group
        );
    }
}
