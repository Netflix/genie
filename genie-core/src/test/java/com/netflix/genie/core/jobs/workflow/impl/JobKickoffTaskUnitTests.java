package com.netflix.genie.core.jobs.workflow.impl;

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
     *
     */
    @Before
    public void setUp() {
        this.executor = Mockito.mock(Executor.class);
        jobKickoffTask = new JobKickoffTask(
            false,
            false,
            this.executor,
            "localhost"
        );
    }

    /**
     * Test the change ownership method for success.
     *
     * @throws IOException If there is any problem.
     * @throws GenieException If there is any problem.
     */
    @Test
    public void testChangeOwnershipOfDirectoryMethodSuccess() throws IOException, GenieException {

        final String user = "user";
        final String dir = "dir";
        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        final List<String> command = Arrays.asList("chown", "-R", user, dir);

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
     * @throws IOException If there is any problem.
     * @throws GenieException If there is any problem.
     */
    @Test(expected = GenieServerException.class)
    public void testChangeOwnershipOfDirectoryMethodFailure() throws IOException, GenieException {

        final String user = "user";
        final String dir = "dir";

        Mockito.when(this.executor.execute(Mockito.any())).thenThrow(IOException.class);
        this.jobKickoffTask.changeOwnershipOfDirectory(
            dir,
            user
        );
    }

    /**
     * Test the create user method for user already exists.
     *
     * @throws IOException If there is any problem.
     * @throws GenieException If there is any problem.
     */
    @Test
    public void testCreateUserMethodSuccessAlreadyExists() throws IOException, GenieException {

        final String user = "user";
        final String group = "group";
        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        final List<String> command = Arrays.asList("id", "-u", user);

        this.jobKickoffTask.createUser(
            user,
            group
        );
        Mockito.verify(this.executor).execute(argumentCaptor.capture());
        Assert.assertArrayEquals(command.toArray(), argumentCaptor.getValue().toStrings());
    }

//    /**
//     * Test the create user method for user already exists.
//     *
//     * @throws IOException If there is any problem.
//     * @throws GenieException If there is any problem.
//     */
//    @Test
//    public void testCreateUserMethodSuccessDoesNotExist() throws IOException, GenieException {
//
//        final String user = "foo";
//        final String group = "group";
//
//        final CommandLine idCheckCommandLine = new CommandLine("id");
//        idCheckCommandLine.addArgument("-u");
//        idCheckCommandLine.addArgument(user);
//
//        Mockito.when(this.executor.execute(Mockito.eq(Mockito.any()))).
// thenThrow(IOException.class, IOException.class);
//
//        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
//        final List<String> command = Arrays.asList("sudo", "useradd", user, "-G", group);
//
//        this.jobKickoffTask.createUser(
//            user,
//            group
//        );
//        Mockito.verify(this.executor, Mockito.times(2)).execute(argumentCaptor.capture());
//        Assert.assertArrayEquals(command.toArray(), argumentCaptor.getAllValues().get(0).toStrings());
//    }
}
