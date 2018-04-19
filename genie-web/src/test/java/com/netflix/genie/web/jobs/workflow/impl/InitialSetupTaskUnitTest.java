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

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.ClusterMetadata;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.CommandMetadata;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.controllers.DtoConverters;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

/**
 * Tests for the InitialSetup task.
 *
 * @author mprimi
 * @since 3.1.0
 */
@Category(UnitTest.class)
public class InitialSetupTaskUnitTest {

    /**
     * Temporary folder job folder.
     */
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    private InitialSetupTask initialSetupTask;

    /**
     * Setup to run before each test.
     */
    @Before
    public void setUp() {
        final MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        Mockito
            .when(registry.timer(Mockito.eq(InitialSetupTask.SETUP_TASK_TIMER_NAME), Mockito.anySet()))
            .thenReturn(Mockito.mock(Timer.class));
        this.initialSetupTask = new InitialSetupTask(registry);
    }

    /**
     * Test the method that sets up the job working directory.
     *
     * @throws Exception if an error occurs
     */
    @Test
    public void testCreateJobDirStructure() throws Exception {
        initialSetupTask.createJobDirStructure(this.tempDir.getRoot().getPath());

        final File genieDirectory = new File(this.tempDir.getRoot(), JobConstants.GENIE_PATH_VAR);
        Assert.assertTrue(genieDirectory.exists());
        Assert.assertTrue(genieDirectory.isDirectory());

        final String[] subDirectoryNames = {
            JobConstants.LOGS_PATH_VAR,
            JobConstants.APPLICATION_PATH_VAR,
            JobConstants.COMMAND_PATH_VAR,
            JobConstants.CLUSTER_PATH_VAR,
        };

        for (final String subDirectoryName : subDirectoryNames) {
            final File genieSubDirectory = new File(genieDirectory, subDirectoryName);
            Assert.assertTrue(genieSubDirectory.exists());
            Assert.assertTrue(genieSubDirectory.isDirectory());
        }

        final String[] emptyFileNames = {
            JobConstants.STDOUT_LOG_FILE_NAME,
            JobConstants.STDERR_LOG_FILE_NAME,
        };

        for (final String emptyFileName : emptyFileNames) {
            final File emptyFile = new File(this.tempDir.getRoot(), emptyFileName);
            Assert.assertTrue(emptyFile.exists());
            Assert.assertTrue(emptyFile.isFile());
        }
    }

    /**
     * Test the methods composing environment variables written to the run script.
     *
     * @throws Exception if an error occurs
     */
    @Test
    public void testCreateEnvironmentVariables() throws Exception {
        final String jobId = UUID.randomUUID().toString();
        final String clusterId = "cluster-id";
        final String clusterName = "cluster-name";
        final String clusterTag1 = "cluster-foo";
        final String clusterTag2 = "cluster-bar";
        final String commandName = "command-name";
        final String commandId = "command-id";
        final String commandTag1 = "cmd-bar";
        final String commandTag2 = "cmd-foo";
        final String jobName = "The Job";
        final int memory = 1000;
        final String cmdCritTag1 = "tagX";
        final String cmdCritTag2 = "tagY";
        final String cmdCritTag3 = "tagZ";
        final String cltCritTag1 = "bar";
        final String cltCritTag2 = "baz";
        final String cltCritTag3 = "foo";

        final Cluster mockCluster = Mockito.mock(Cluster.class);
        Mockito.when(mockCluster.getId()).thenReturn(clusterId);

        final ClusterMetadata clusterMetadata = Mockito.mock(ClusterMetadata.class);
        Mockito.when(mockCluster.getMetadata()).thenReturn(clusterMetadata);
        Mockito.when(clusterMetadata.getName()).thenReturn(clusterName);
        Mockito.when(clusterMetadata.getTags()).thenReturn(Sets.newHashSet(clusterTag1, clusterTag2));

        final Command mockCommand = Mockito.mock(Command.class);
        Mockito.when(mockCommand.getId()).thenReturn(commandId);

        final CommandMetadata commandMetadata = Mockito.mock(CommandMetadata.class);
        Mockito.when(mockCommand.getMetadata()).thenReturn(commandMetadata);
        Mockito.when(commandMetadata.getName()).thenReturn(commandName);
        Mockito.when(commandMetadata.getTags()).thenReturn(Sets.newHashSet(commandTag2, commandTag1));

        final JobRequest mockJobRequest = Mockito.mock(JobRequest.class);
        Mockito
            .when(mockJobRequest.getCommandCriteria())
            .thenReturn(
                Sets.newHashSet(Arrays.asList(cmdCritTag3, cmdCritTag2, cmdCritTag1))
            );
        Mockito
            .when(mockJobRequest.getClusterCriterias())
            .thenReturn(
                Arrays.asList(
                    new ClusterCriteria(Sets.newHashSet(cltCritTag3, cltCritTag1, cltCritTag2)),
                    new ClusterCriteria(Sets.newHashSet(cltCritTag3, cltCritTag1)),
                    new ClusterCriteria(Sets.newHashSet(cltCritTag3))
                )
            );

        final StringWriter mockWriter = new StringWriter();
        this.initialSetupTask.createJobDirEnvironmentVariables(mockWriter, this.tempDir.getRoot().getAbsolutePath());
        this.initialSetupTask.createApplicationEnvironmentVariables(mockWriter);
        this.initialSetupTask.createCommandEnvironmentVariables(mockWriter, mockCommand);
        this.initialSetupTask.createClusterEnvironmentVariables(mockWriter, mockCluster);
        this.initialSetupTask.createJobEnvironmentVariables(mockWriter, jobId, jobName, memory);
        this.initialSetupTask.createJobRequestEnvironmentVariables(mockWriter, mockJobRequest);

        final String expectedOutput = ""
            + "export GENIE_JOB_DIR=\"" + tempDir.getRoot().getAbsolutePath() + "\"\n"
            + "\n"
            + "export GENIE_APPLICATION_DIR=\"${GENIE_JOB_DIR}/genie/applications\"\n"
            + "\n"
            + "export GENIE_COMMAND_DIR=\"${GENIE_JOB_DIR}/genie/command/" + commandId + "\"\n"
            + "\n"
            + "export GENIE_COMMAND_ID=\"" + commandId + "\"\n"
            + "\n"
            + "export GENIE_COMMAND_NAME=\"" + commandName + "\"\n"
            + "\n"
            + "export GENIE_COMMAND_TAGS=\"" + commandTag1 + "," + commandTag2 + "," + DtoConverters.GENIE_ID_PREFIX
            + commandId + "," + DtoConverters.GENIE_NAME_PREFIX + commandName + "\"\n"
            + "\n"
            + "export GENIE_CLUSTER_DIR=\"${GENIE_JOB_DIR}/genie/cluster/" + clusterId + "\"\n"
            + "\n"
            + "export GENIE_CLUSTER_ID=\"" + clusterId + "\"\n"
            + "\n"
            + "export GENIE_CLUSTER_NAME=\"" + clusterName + "\"\n"
            + "\n"
            + "export GENIE_CLUSTER_TAGS=\"" + clusterTag2 + "," + clusterTag1 + "," + DtoConverters.GENIE_ID_PREFIX
            + clusterId + "," + DtoConverters.GENIE_NAME_PREFIX + clusterName + "\"\n"
            + "\n"
            + "export GENIE_JOB_ID=\"" + jobId + "\"\n"
            + "\n"
            + "export GENIE_JOB_NAME=\"" + jobName + "\"\n"
            + "\n"
            + "export GENIE_JOB_MEMORY=" + memory + "\n"
            + "\n"
            + "export GENIE_REQUESTED_COMMAND_TAGS=\"" + cmdCritTag1 + "," + cmdCritTag2 + "," + cmdCritTag3 + "\"\n"
            + "\n"
            + "export GENIE_REQUESTED_CLUSTER_TAGS=\"["
            + "[" + cltCritTag1 + "," + cltCritTag2 + "," + cltCritTag3 + "],"
            + "[" + cltCritTag1 + "," + cltCritTag3 + "],"
            + "[" + cltCritTag3 + "]"
            + "]\"" + "\n"
            + "export GENIE_REQUESTED_CLUSTER_TAGS_0=\"" + cltCritTag1 + "," + cltCritTag2 + "," + cltCritTag3 + "\"\n"
            + "export GENIE_REQUESTED_CLUSTER_TAGS_1=\"" + cltCritTag1 + "," + cltCritTag3 + "\"\n"
            + "export GENIE_REQUESTED_CLUSTER_TAGS_2=\"" + cltCritTag3 + "\"\n"
            + "\n";

        Assert.assertEquals(expectedOutput, mockWriter.getString());
    }

    /**
     * Test helper method to serialize cluster/command tags.
     */
    @Test
    public void testTagsToString() {

        Assert.assertEquals("", initialSetupTask.tagsToString(null));

        final String[][] inputs = new String[][]{
            {},
            {"some.tag:t"},
            {"foo", "bar"},
            {"bar", "foo"},
            {"foo", "bar", "tag,with,commas"},
            {"foo", "bar", "tag with spaces"},
            {"foo", "bar", "tag\nwith\nnewlines"},
            {"foo", "bar", "\"tag-with-double-quotes\""},
            {"foo", "bar", "\'tag-with-single-quotes\'"},
        };

        final String[] outputs = {
            "",
            "some.tag:t",
            "bar,foo",
            "bar,foo",
            "bar,foo,tag,with,commas", // Commas in tags are not escaped
            "bar,foo,tag with spaces",
            "bar,foo,tag\nwith\nnewlines",
            "\"tag-with-double-quotes\",bar,foo",
            "\'tag-with-single-quotes\',bar,foo",
        };

        Assert.assertEquals(inputs.length, outputs.length);
        for (int i = 0; i < inputs.length; i++) {
            final String expectedOutputString = outputs[i];
            final HashSet<String> tags = new HashSet<>(Arrays.asList(inputs[i]));
            Assert.assertEquals(expectedOutputString, this.initialSetupTask.tagsToString(tags));
        }
    }

    private static class StringWriter extends Writer {

        private final StringBuilder stringBuilder = new StringBuilder();

        @Override
        public void write(@Nonnull final char[] cbuf, final int off, final int len) throws IOException {
            this.stringBuilder.append(cbuf, off, len);
        }

        @Override
        public void flush() throws IOException {
            // noop
        }

        @Override
        public void close() throws IOException {
            // noop
        }

        public String getString() {
            return this.stringBuilder.toString();
        }
    }
}
