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
package com.netflix.genie.common.dto;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for the JobRequest class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobRequestUnitTests {

    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final String COMMAND_ARGS = UUID.randomUUID().toString();
    private static final List<ClusterCriteria> CLUSTER_CRITERIAS = Lists.newArrayList(
        new ClusterCriteria(
            Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        ),
        new ClusterCriteria(
            Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        ),
        new ClusterCriteria(
            Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        )
    );
    private static final Set<String> COMMAND_CRITERIA = Sets.newHashSet(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()
    );

    /**
     * Test to make sure can build a valid JobRequest using the builder.
     */
    @Test
    public void canBuildJobRequest() {
        final JobRequest request
            = new JobRequest.Builder(NAME, USER, VERSION, COMMAND_ARGS, CLUSTER_CRITERIAS, COMMAND_CRITERIA).build();
        Assert.assertThat(request.getName(), Matchers.is(NAME));
        Assert.assertThat(request.getUser(), Matchers.is(USER));
        Assert.assertThat(request.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(request.getCommandArgs(), Matchers.is(COMMAND_ARGS));
        Assert.assertThat(request.getClusterCriterias(), Matchers.is(CLUSTER_CRITERIAS));
        Assert.assertThat(request.getCommandCriteria(), Matchers.is(COMMAND_CRITERIA));
        Assert.assertThat(request.getCpu(), Matchers.is(1));
        Assert.assertThat(request.isDisableLogArchival(), Matchers.is(false));
        Assert.assertThat(request.getEmail(), Matchers.nullValue());
        Assert.assertThat(request.getDependencies(), Matchers.empty());
        Assert.assertThat(request.getGroup(), Matchers.nullValue());
        Assert.assertThat(request.getMemory(), Matchers.is(1536));
        Assert.assertThat(request.getSetupFile(), Matchers.nullValue());
        Assert.assertThat(request.getCreated(), Matchers.nullValue());
        Assert.assertThat(request.getDescription(), Matchers.nullValue());
        Assert.assertThat(request.getId(), Matchers.nullValue());
        Assert.assertThat(request.getTags(), Matchers.empty());
        Assert.assertThat(request.getUpdated(), Matchers.nullValue());
    }

    /**
     * Test to make sure can build a valid JobRequest with optional parameters.
     */
    @Test
    public void canBuildJobRequestWithOptionals() {
        final JobRequest.Builder builder
            = new JobRequest.Builder(NAME, USER, VERSION, COMMAND_ARGS, CLUSTER_CRITERIAS, COMMAND_CRITERIA);

        final int cpu = 5;
        builder.withCpu(cpu);

        final boolean disableLogArchival = true;
        builder.withDisableLogArchival(disableLogArchival);

        final String email = UUID.randomUUID().toString() + "@netflix.com";
        builder.withEmail(email);

        final Set<String> dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        builder.withDependencies(dependencies);

        final String group = UUID.randomUUID().toString();
        builder.withGroup(group);

        final int memory = 2048;
        builder.withMemory(memory);

        final String setupFile = UUID.randomUUID().toString();
        builder.withSetupFile(setupFile);

        final Date created = new Date();
        builder.withCreated(created);

        final String description = UUID.randomUUID().toString();
        builder.withDescription(description);

        final String id = UUID.randomUUID().toString();
        builder.withId(id);

        final Set<String> tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        builder.withTags(tags);

        final Date updated = new Date();
        builder.withUpdated(updated);

        final JobRequest request = builder.build();
        Assert.assertThat(request.getName(), Matchers.is(NAME));
        Assert.assertThat(request.getUser(), Matchers.is(USER));
        Assert.assertThat(request.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(request.getCommandArgs(), Matchers.is(COMMAND_ARGS));
        Assert.assertThat(request.getClusterCriterias(), Matchers.is(CLUSTER_CRITERIAS));
        Assert.assertThat(request.getCommandCriteria(), Matchers.is(COMMAND_CRITERIA));
        Assert.assertThat(request.getCpu(), Matchers.is(cpu));
        Assert.assertThat(request.isDisableLogArchival(), Matchers.is(disableLogArchival));
        Assert.assertThat(request.getEmail(), Matchers.is(email));
        Assert.assertThat(request.getDependencies(), Matchers.is(dependencies));
        Assert.assertThat(request.getGroup(), Matchers.is(group));
        Assert.assertThat(request.getMemory(), Matchers.is(memory));
        Assert.assertThat(request.getSetupFile(), Matchers.is(setupFile));
        Assert.assertThat(request.getCreated(), Matchers.is(created));
        Assert.assertThat(request.getDescription(), Matchers.is(description));
        Assert.assertThat(request.getId(), Matchers.is(id));
        Assert.assertThat(request.getTags(), Matchers.is(tags));
        Assert.assertThat(request.getUpdated(), Matchers.is(updated));
    }

    /**
     * Test to make sure a JobRequest can be successfully built when nulls are inputted.
     */
    @Test
    public void canBuildJobRequestWithNulls() {
        final JobRequest.Builder builder
            = new JobRequest.Builder(NAME, USER, VERSION, COMMAND_ARGS, null, null);
        builder.withEmail(null);
        builder.withDependencies(null);
        builder.withGroup(null);
        builder.withSetupFile(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(null);
        builder.withTags(null);
        builder.withUpdated(null);

        final JobRequest request = builder.build();
        Assert.assertThat(request.getName(), Matchers.is(NAME));
        Assert.assertThat(request.getUser(), Matchers.is(USER));
        Assert.assertThat(request.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(request.getCommandArgs(), Matchers.is(COMMAND_ARGS));
        Assert.assertThat(request.getClusterCriterias(), Matchers.empty());
        Assert.assertThat(request.getCommandCriteria(), Matchers.empty());
        Assert.assertThat(request.getCpu(), Matchers.is(1));
        Assert.assertThat(request.isDisableLogArchival(), Matchers.is(false));
        Assert.assertThat(request.getEmail(), Matchers.nullValue());
        Assert.assertThat(request.getDependencies(), Matchers.empty());
        Assert.assertThat(request.getGroup(), Matchers.nullValue());
        Assert.assertThat(request.getMemory(), Matchers.is(1536));
        Assert.assertThat(request.getSetupFile(), Matchers.nullValue());
        Assert.assertThat(request.getCreated(), Matchers.nullValue());
        Assert.assertThat(request.getDescription(), Matchers.nullValue());
        Assert.assertThat(request.getId(), Matchers.nullValue());
        Assert.assertThat(request.getTags(), Matchers.empty());
        Assert.assertThat(request.getUpdated(), Matchers.nullValue());
    }

    /**
     * Test equals.
     */
    @Test
    public void canFindEquality() {
        final JobRequest.Builder builder
            = new JobRequest.Builder(NAME, USER, VERSION, COMMAND_ARGS, null, null);
        builder.withEmail(null);
        builder.withDependencies(null);
        builder.withGroup(null);
        builder.withSetupFile(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);

        final JobRequest jobRequest1 = builder.build();
        final JobRequest jobRequest2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobRequest jobRequest3 = builder.build();

        Assert.assertTrue(jobRequest1.equals(jobRequest2));
        Assert.assertTrue(jobRequest2.equals(jobRequest1));
        Assert.assertFalse(jobRequest1.equals(jobRequest3));
    }

    /**
     * Test hash code.
     */
    @Test
    public void canUseHashCode() {
        final JobRequest.Builder builder
            = new JobRequest.Builder(NAME, USER, VERSION, COMMAND_ARGS, null, null);
        builder.withEmail(null);
        builder.withDependencies(null);
        builder.withGroup(null);
        builder.withSetupFile(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);

        final JobRequest jobRequest1 = builder.build();
        final JobRequest jobRequest2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobRequest jobRequest3 = builder.build();

        Assert.assertEquals(jobRequest1.hashCode(), jobRequest2.hashCode());
        Assert.assertNotEquals(jobRequest1.hashCode(), jobRequest3.hashCode());
    }
}
