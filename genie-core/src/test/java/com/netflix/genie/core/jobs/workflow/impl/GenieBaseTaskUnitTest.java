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
package com.netflix.genie.core.jobs.workflow.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.jobs.AdminResources;
import com.netflix.genie.core.jobs.FileType;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for GenieBaseTask.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieBaseTaskUnitTest {

    private static final String INVALID_FILE_PATH = "/foo/bar";
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
     * Test the buildLocalPath method for config file type for applications.
     *
     * @throws GenieException if there is a problem.
     */
    @Test
    public void testBuildLocalPathConfigApplication() throws GenieException {
        final String localPath = this.genieBaseTask.buildLocalFilePath(
            "dirpath",
            "id",
            "filepath/filename",
            FileType.CONFIG,
            AdminResources.APPLICATION
        );

        Assert.assertEquals("dirpath/genie/applications/id/config/filename", localPath);
    }

    /**
     * Test the buildLocalPath method for config file type for applications.
     *
     * @throws GenieException if there is a problem.
     */
    @Test
    public void testBuildLocalPathSetupApplication() throws GenieException {
        final String localPath = this.genieBaseTask.buildLocalFilePath(
            "dirpath",
            "id",
            "filepath/filename",
            FileType.SETUP,
            AdminResources.APPLICATION
        );

        Assert.assertEquals("dirpath/genie/applications/id/filename", localPath);
    }

    /**
     * Test the buildLocalPath method for dependency file type for applications.
     *
     * @throws GenieException if there is a problem.
     */
    @Test
    public void testBuildLocalPathDependenciesApplication() throws GenieException {
        final String localPath = this.genieBaseTask.buildLocalFilePath(
            "dirpath",
            "id",
            "filepath/filename",
            FileType.DEPENDENCIES,
            AdminResources.APPLICATION
        );

        Assert.assertEquals("dirpath/genie/applications/id/dependencies/filename", localPath);
    }

    /**
     * Test the buildLocalPath method for config file type for command.
     *
     * @throws GenieException if there is a problem.
     */
    @Test
    public void testBuildLocalPathConfigCommand() throws GenieException {
        final String localPath = this.genieBaseTask.buildLocalFilePath(
            "dirpath",
            "id",
            "filepath/filename",
            FileType.CONFIG,
            AdminResources.COMMAND
        );

        Assert.assertEquals("dirpath/genie/command/id/config/filename", localPath);
    }

    /**
     * Test the buildLocalPath method for config file type for command.
     *
     * @throws GenieException if there is a problem.
     */
    @Test
    public void testBuildLocalPathSetupCommand() throws GenieException {
        final String localPath = this.genieBaseTask.buildLocalFilePath(
            "dirpath",
            "id",
            "filepath/filename",
            FileType.SETUP,
            AdminResources.COMMAND
        );

        Assert.assertEquals("dirpath/genie/command/id/filename", localPath);
    }

    /**
     * Test the buildLocalPath method for dependency file type for command.
     *
     * @throws GenieException if there is a problem.
     */
    @Test
    public void testBuildLocalPathDependenciesCommand() throws GenieException {
        final String localPath = this.genieBaseTask.buildLocalFilePath(
            "dirpath",
            "id",
            "filepath/filename",
            FileType.DEPENDENCIES,
            AdminResources.COMMAND
        );

        Assert.assertEquals("dirpath/genie/command/id/dependencies/filename", localPath);
    }

    /**
     * Test the buildLocalPath method for config file type for cluster.
     *
     * @throws GenieException if there is a problem.
     */
    @Test
    public void testBuildLocalPathConfigCluster() throws GenieException {
        final String localPath = this.genieBaseTask.buildLocalFilePath(
            "dirpath",
            "id",
            "filepath/filename",
            FileType.CONFIG,
            AdminResources.CLUSTER
        );

        Assert.assertEquals("dirpath/genie/cluster/id/config/filename", localPath);
    }

    /**
     * Test the buildLocalPath method for config file type for cluster.
     *
     * @throws GenieException if there is a problem.
     */
    @Test
    public void testBuildLocalPathSetupCluster() throws GenieException {
        final String localPath = this.genieBaseTask.buildLocalFilePath(
            "dirpath",
            "id",
            "filepath/filename",
            FileType.SETUP,
            AdminResources.CLUSTER
        );

        Assert.assertEquals("dirpath/genie/cluster/id/filename", localPath);
    }

    /**
     * Test the buildLocalPath method for dependency file type for command.
     *
     * @throws GenieException if there is a problem.
     */
    @Test
    public void testBuildLocalPathDependenciesCluster() throws GenieException {
        final String localPath = this.genieBaseTask.buildLocalFilePath(
            "dirpath",
            "id",
            "filepath/filename",
            FileType.DEPENDENCIES,
            AdminResources.CLUSTER
        );

        Assert.assertEquals("dirpath/genie/cluster/id/dependencies/filename", localPath);
    }

    /**
     * Tests the execute method when no JobExecutionEnvironment present in the map.
     *
     * @throws GenieException If there is any problem.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testExecuteMethodNoJobExecution() throws GenieException {
        final Map<String, Object> context = new HashMap<>();
        this.genieBaseTask.executeTask(context);
    }
}
