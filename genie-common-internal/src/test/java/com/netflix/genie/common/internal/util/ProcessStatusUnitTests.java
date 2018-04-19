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
package com.netflix.genie.common.internal.util;

import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for the ProcessStatus enum.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Category(UnitTest.class)
public class ProcessStatusUnitTests {

    /**
     * Test to make sure the error codes are correct.
     */
    @Test
    public void testErrorCodes() {
        Assert.assertEquals(-1, ProcessStatus.JOB_INTERRUPTED.getExitCode());
        Assert.assertEquals(0, ProcessStatus.SUCCESS.getExitCode());
        Assert.assertEquals(201, ProcessStatus.MKDIR_JAR_FAILURE.getExitCode());
        Assert.assertEquals(202, ProcessStatus.MKDIR_CONF_FAILURE.getExitCode());
        Assert.assertEquals(203, ProcessStatus.HADOOP_LOCAL_CONF_COPY_FAILURE.getExitCode());
        Assert.assertEquals(204, ProcessStatus.UPDATE_CORE_SITE_XML_FAILURE.getExitCode());
        Assert.assertEquals(205, ProcessStatus.ENV_VARIABLES_SOURCE_AND_SETUP_FAILURE.getExitCode());
        Assert.assertEquals(206, ProcessStatus.CLUSTER_CONF_FILES_COPY_FAILURE.getExitCode());
        Assert.assertEquals(207, ProcessStatus.COMMAND_CONF_FILES_COPY_FAILURE.getExitCode());
        Assert.assertEquals(208, ProcessStatus.APPLICATION_CONF_FILES_COPY_FAILURE.getExitCode());
        Assert.assertEquals(209, ProcessStatus.APPLICATION_JAR_FILES_COPY_FAILURE.getExitCode());
        Assert.assertEquals(210, ProcessStatus.JOB_DEPENDENCIES_COPY_FAILURE.getExitCode());
        Assert.assertEquals(211, ProcessStatus.JOB_KILLED.getExitCode());
        Assert.assertEquals(212, ProcessStatus.ZOMBIE_JOB.getExitCode());
        Assert.assertEquals(213, ProcessStatus.COMMAND_RUN_FAILURE.getExitCode());
    }

    /**
     * Test to make sure the messages are correct.
     */
    @Test
    public void testMessages() {
        Assert.assertEquals("Job execution interrupted.",
                ProcessStatus.JOB_INTERRUPTED.getMessage());
        Assert.assertEquals("Success.",
                ProcessStatus.SUCCESS.getMessage());
        Assert.assertEquals("Failed to create job jar dir.",
                ProcessStatus.MKDIR_JAR_FAILURE.getMessage());
        Assert.assertEquals("Failed to create job conf dir.",
                ProcessStatus.MKDIR_CONF_FAILURE.getMessage());
        Assert.assertEquals("Failed copying Hadoop files from local conf dir to current job conf dir.",
                ProcessStatus.HADOOP_LOCAL_CONF_COPY_FAILURE.getMessage());
        Assert.assertEquals("Failed updating core-site.xml to add certain parameters.",
                ProcessStatus.UPDATE_CORE_SITE_XML_FAILURE.getMessage());
        Assert.assertEquals("Failed while sourcing resource envProperty files.",
                ProcessStatus.ENV_VARIABLES_SOURCE_AND_SETUP_FAILURE.getMessage());
        Assert.assertEquals("Failed copying cluster conf files from S3",
                ProcessStatus.CLUSTER_CONF_FILES_COPY_FAILURE.getMessage());
        Assert.assertEquals("Failed copying command conf files from S3",
                ProcessStatus.COMMAND_CONF_FILES_COPY_FAILURE.getMessage());
        Assert.assertEquals("Failed copying application conf files from S3",
                ProcessStatus.APPLICATION_CONF_FILES_COPY_FAILURE.getMessage());
        Assert.assertEquals("Failed copying application jar files from S3",
                ProcessStatus.APPLICATION_JAR_FILES_COPY_FAILURE.getMessage());
        Assert.assertEquals("Job failed copying dependent files.",
                ProcessStatus.JOB_DEPENDENCIES_COPY_FAILURE.getMessage());
        Assert.assertEquals("Job killed after it exceeded system limits",
                ProcessStatus.JOB_KILLED.getMessage());
        Assert.assertEquals("Job has been marked as a zombie",
                ProcessStatus.ZOMBIE_JOB.getMessage());
        Assert.assertEquals("Command failed with non-zero exit code.",
                ProcessStatus.COMMAND_RUN_FAILURE.getMessage());
    }

    /**
     * Test to make sure the parse method works for valid cases.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testParse() throws GeniePreconditionException {
        Assert.assertEquals(ProcessStatus.JOB_INTERRUPTED,
                ProcessStatus.parse(-1));
        Assert.assertEquals(ProcessStatus.SUCCESS,
                ProcessStatus.parse(0));
        Assert.assertEquals(ProcessStatus.MKDIR_JAR_FAILURE,
                ProcessStatus.parse(201));
        Assert.assertEquals(ProcessStatus.MKDIR_CONF_FAILURE,
                ProcessStatus.parse(202));
        Assert.assertEquals(ProcessStatus.HADOOP_LOCAL_CONF_COPY_FAILURE,
                ProcessStatus.parse(203));
        Assert.assertEquals(ProcessStatus.UPDATE_CORE_SITE_XML_FAILURE,
                ProcessStatus.parse(204));
        Assert.assertEquals(ProcessStatus.ENV_VARIABLES_SOURCE_AND_SETUP_FAILURE,
                ProcessStatus.parse(205));
        Assert.assertEquals(ProcessStatus.CLUSTER_CONF_FILES_COPY_FAILURE,
                ProcessStatus.parse(206));
        Assert.assertEquals(ProcessStatus.COMMAND_CONF_FILES_COPY_FAILURE,
                ProcessStatus.parse(207));
        Assert.assertEquals(ProcessStatus.APPLICATION_CONF_FILES_COPY_FAILURE,
                ProcessStatus.parse(208));
        Assert.assertEquals(ProcessStatus.APPLICATION_JAR_FILES_COPY_FAILURE,
                ProcessStatus.parse(209));
        Assert.assertEquals(ProcessStatus.JOB_DEPENDENCIES_COPY_FAILURE,
                ProcessStatus.parse(210));
        Assert.assertEquals(ProcessStatus.JOB_KILLED,
                ProcessStatus.parse(211));
        Assert.assertEquals(ProcessStatus.ZOMBIE_JOB,
                ProcessStatus.parse(212));
        Assert.assertEquals(ProcessStatus.COMMAND_RUN_FAILURE,
                ProcessStatus.parse(213));
    }

    /**
     * Test to make sure the parse method works for valid cases.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testParseBadErrorCode() throws GeniePreconditionException {
        Assert.assertEquals(ProcessStatus.COMMAND_RUN_FAILURE,
                ProcessStatus.parse(-2490354));
    }
}
