package com.netflix.genie.core.jobs.workflow;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jobs.workflow.impl.GenieBaseTask;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for GenieBaseTask.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieBaseTaskUnitTest {

    private GenieBaseTask genieBaseTask;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        // Mock an instance of the Base class so that you can call the methods with creating a concrete class.
        this.genieBaseTask = Mockito.mock(GenieBaseTask.class, Mockito.CALLS_REAL_METHODS);
    }

    /**
     * Tests the getFileName method functionality.
     *
     * @throws GenieException if there is any problem
     */
    @Test
    public void testGetFileNameFromPath() throws GenieException {
        final String testFilePath = "fs://foo/bar/name";
        Assert.assertEquals("name", genieBaseTask.getFileNameFromPath(testFilePath));
    }

    /**
     * Tests the executeBashCommand method functionality.
     *
     * @throws GenieException if there is any problem
     */
    @Test
    public void testExecuteBashCommand() throws GenieException {
        final List command = new ArrayList<>();
        command.add("ls");
        command.add("-l");

        this.genieBaseTask.executeBashCommand(command, null);
    }

    /**
     * Tests the executeBashCommand method functionality.
     *
     * @throws GenieException if there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testExecuteBashCommandWithInvalidCommand() throws GenieException {
        final List command = new ArrayList<>();
        command.add("foo");
        this.genieBaseTask.executeBashCommand(command, null);
    }

    /**
     * Tests the executeBashCommand method functionality.
     *
     * @throws GenieException if there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void testExecuteBashCommandWithNonZeroReturnCode() throws GenieException {
        final List command = new ArrayList<>();
        command.add("ls");
        command.add("foo");
        this.genieBaseTask.executeBashCommand(command, null);
    }

    /**
     * Tests the executeBashCommand method functionality.
     *
     * @throws GenieException if there is any problem
     */
    @Test
    public void testExecuteBashCommandWithWorkingDirSet() throws GenieException {
        final List command = new ArrayList<>();
        command.add("ls");
        command.add("-l");

        final String workingDir = "/tmp";

        this.genieBaseTask.executeBashCommand(command, workingDir);
    }

//    /**
//     * Tests the executeBashCommand method functionality.
//     *
//     * @throws GenieException if there is any problem
//     */
//    @Test (expected = GenieServerException.class)
//    public void testExecuteBashCommandWithInvalidWorkingDirSet() throws GenieException {
//        final List command = new ArrayList<>();
//        command.add("ls");
//        command.add("-l");
//
//        final String workingDir = "/foo";
//        this.genieBaseTask.executeBashCommand(command, workingDir);
//    }
}
