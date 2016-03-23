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
package com.netflix.genie.core.jpa.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for the JobRequestEntity class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobRequestEntityUnitTests {

    private static final String EMPTY_JSON_ARRAY = "[]";

    private JobRequestEntity entity;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.entity = new JobRequestEntity();
    }

    /**
     * Make sure can successfully construct a JobRequestEntity.
     *
     * @throws GenieException on error
     */
    @Test
    public void canConstruct() throws GenieException {
        Assert.assertThat(this.entity.getId(), Matchers.nullValue());
        Assert.assertThat(this.entity.getName(), Matchers.nullValue());
        Assert.assertThat(this.entity.getUser(), Matchers.nullValue());
        Assert.assertThat(this.entity.getVersion(), Matchers.nullValue());
        Assert.assertThat(this.entity.getDescription(), Matchers.nullValue());
        Assert.assertThat(this.entity.getCreated(), Matchers.notNullValue());
        Assert.assertThat(this.entity.getUpdated(), Matchers.notNullValue());
        Assert.assertThat(this.entity.getTags(), Matchers.empty());
        Assert.assertThat(this.entity.getClientHost(), Matchers.nullValue());
        Assert.assertThat(this.entity.getClusterCriterias(), Matchers.is(EMPTY_JSON_ARRAY));
        Assert.assertThat(this.entity.getClusterCriteriasAsList(), Matchers.empty());
        Assert.assertThat(this.entity.getCommandArgs(), Matchers.nullValue());
        Assert.assertThat(this.entity.getCommandCriteria(), Matchers.is(EMPTY_JSON_ARRAY));
        Assert.assertThat(this.entity.getCommandCriteriaAsSet(), Matchers.empty());
        Assert.assertThat(this.entity.getCpu(), Matchers.is(1));
        Assert.assertThat(this.entity.getEmail(), Matchers.nullValue());
        Assert.assertThat(this.entity.getDependencies(), Matchers.is(EMPTY_JSON_ARRAY));
        Assert.assertThat(this.entity.getDependenciesAsSet(), Matchers.empty());
        Assert.assertThat(this.entity.getGroup(), Matchers.nullValue());
        Assert.assertThat(this.entity.getJob(), Matchers.nullValue());
        Assert.assertThat(this.entity.getMemory(), Matchers.is(1536));
        Assert.assertThat(this.entity.getSetupFile(), Matchers.nullValue());
        Assert.assertThat(this.entity.getTags(), Matchers.empty());
        Assert.assertFalse(this.entity.isDisableLogArchival());
        Assert.assertThat(this.entity.getApplicationsAsList(), Matchers.empty());
        Assert.assertThat(this.entity.getApplications(), Matchers.is(EMPTY_JSON_ARRAY));
        Assert.assertThat(this.entity.getTimeout(), Matchers.is(604800));
    }

    /**
     * Make sure can set the group for the job.
     */
    @Test
    public void canSetGroup() {
        final String group = UUID.randomUUID().toString();
        this.entity.setGroup(group);
        Assert.assertThat(this.entity.getGroup(), Matchers.is(group));
    }

    /**
     * Make sure can set the list of cluster criteria's for the job.
     *
     * @throws GenieException on error
     */
    @Test
    public void canSetClusterCriteriasFromList() throws GenieException {
        final List<ClusterCriteria> clusterCriteriaList = Lists.newArrayList(
            new ClusterCriteria(
                Sets.newHashSet(
                    "one",
                    "two",
                    "three"
                )
            ),
            new ClusterCriteria(
                Sets.newHashSet(
                    "four",
                    "five",
                    "six"
                )
            ),
            new ClusterCriteria(
                Sets.newHashSet(
                    "seven",
                    "eight",
                    "nine"
                )
            )
        );

        this.entity.setClusterCriteriasFromList(clusterCriteriaList);
        Assert.assertThat(this.entity.getClusterCriterias(), Matchers.notNullValue());
        final List<ClusterCriteria> criterias = this.entity.getClusterCriteriasAsList();
        Assert.assertThat(criterias.size(), Matchers.is(3));
        Assert.assertThat(criterias.get(0).getTags(), Matchers.containsInAnyOrder("one", "two", "three"));
        Assert.assertThat(criterias.get(1).getTags(), Matchers.containsInAnyOrder("four", "five", "six"));
        Assert.assertThat(criterias.get(2).getTags(), Matchers.containsInAnyOrder("seven", "eight", "nine"));

        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(this.entity.getClusterCriterias());
        } catch (final IOException ioe) {
            Assert.fail();
        }
    }

    /**
     * Make sure the getter setter for the entity class works for JPA for cluster criterias.
     */
    @Test
    public void canSetClusterCriterias() {
        final String clusterCriterias = UUID.randomUUID().toString();
        this.entity.setClusterCriterias(clusterCriterias);
        Assert.assertThat(this.entity.getClusterCriterias(), Matchers.is(clusterCriterias));
    }

    /**
     * Make sure can set the command args for the job.
     */
    @Test
    public void canSetCommandArgs() {
        final String commandArgs = UUID.randomUUID().toString();
        this.entity.setCommandArgs(commandArgs);
        Assert.assertThat(this.entity.getCommandArgs(), Matchers.is(commandArgs));
    }

    /**
     * Make sure can set the file dependencies for the job.
     *
     * @throws GenieException on error
     */
    @Test
    public void canSetFileDependenciesFromSet() throws GenieException {
        final Set<String> fileDependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        this.entity.setDependenciesFromSet(fileDependencies);
        Assert.assertThat(this.entity.getDependenciesAsSet(), Matchers.is(fileDependencies));
        Assert.assertThat(this.entity.getDependencies(), Matchers.notNullValue());

        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(this.entity.getDependencies());
        } catch (final IOException ioe) {
            Assert.fail();
        }

        this.entity.setDependenciesFromSet(null);
        Assert.assertThat(this.entity.getDependenciesAsSet(), Matchers.empty());
        Assert.assertThat(this.entity.getDependencies(), Matchers.is("[]"));
    }

    /**
     * Make sure can set the file dependencies.
     */
    @Test
    public void canSetFileDependencies() {
        final String fileDependencies = UUID.randomUUID().toString();
        this.entity.setDependencies(fileDependencies);
        Assert.assertThat(this.entity.getDependencies(), Matchers.is(fileDependencies));
    }

    /**
     * Make sure can set whether to disable logs or not.
     */
    @Test
    public void canSetDisableLogArchival() {
        this.entity.setDisableLogArchival(true);
        Assert.assertTrue(this.entity.isDisableLogArchival());
    }

    /**
     * Make sure can set the email address of the user.
     */
    @Test
    public void canSetEmail() {
        final String email = UUID.randomUUID().toString();
        this.entity.setEmail(email);
        Assert.assertThat(this.entity.getEmail(), Matchers.is(email));
    }

    /**
     * Make sure can set the client host name the request came from.
     */
    @Test
    public void canSetClientHost() {
        final String clientHost = UUID.randomUUID().toString();
        this.entity.setClientHost(clientHost);
        Assert.assertThat(this.entity.getClientHost(), Matchers.is(clientHost));
    }

    /**
     * Make sure can set the command criteria for the job.
     *
     * @throws GenieException on error
     */
    @Test
    public void canSetCommandCriteriaFromSet() throws GenieException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        this.entity.setCommandCriteriaFromSet(commandCriteria);
        Assert.assertThat(this.entity.getCommandCriteria(), Matchers.notNullValue());
        Assert.assertThat(this.entity.getCommandCriteriaAsSet(), Matchers.is(commandCriteria));

        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(this.entity.getCommandCriteria());
        } catch (final IOException ioe) {
            Assert.fail();
        }
    }

    /**
     * Make sure can set the command criteria.
     */
    @Test
    public void canSetCommandCriteria() {
        final String commandCriteria = UUID.randomUUID().toString();
        this.entity.setCommandCriteria(commandCriteria);
        Assert.assertThat(this.entity.getCommandCriteria(), Matchers.is(commandCriteria));
    }

    /**
     * Make sure can set the setup file.
     */
    @Test
    public void canSetSetupFile() {
        final String setupFile = UUID.randomUUID().toString();
        this.entity.setSetupFile(setupFile);
        Assert.assertThat(this.entity.getSetupFile(), Matchers.is(setupFile));
    }

    /**
     * Make sure can set the tags for the job.
     *
     * @throws GenieException on error
     */
    @Test
    public void canSetTags() throws GenieException {
        final Set<String> tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        this.entity.setTags(tags);
        Assert.assertThat(this.entity.getTags(), Matchers.is(tags));

        this.entity.setTags(null);
        Assert.assertThat(this.entity.getTags(), Matchers.empty());
    }

    /**
     * Make sure can set the number of cpu's to use for the job.
     */
    @Test
    public void canSetCpu() {
        final int cpu = 16;
        this.entity.setCpu(cpu);
        Assert.assertThat(this.entity.getCpu(), Matchers.is(cpu));
    }

    /**
     * Make sure can set the amount of memory to use for the job.
     */
    @Test
    public void canSetMemory() {
        final int memory = 2048;
        this.entity.setMemory(memory);
        Assert.assertThat(this.entity.getMemory(), Matchers.is(memory));
    }

    /**
     * Make sure can set the job that was executed for this request.
     */
    @Test
    public void canSetJob() {
        final JobEntity job = new JobEntity();
        this.entity.setJob(job);
        Assert.assertThat(this.entity.getJob(), Matchers.is(job));
    }

    /**
     * Make sure the entity class sets the applications right.
     */
    @Test
    public void canSetApplications() {
        final String applications = UUID.randomUUID().toString();
        this.entity.setApplications(applications);
        Assert.assertThat(this.entity.getApplications(), Matchers.is(applications));
    }

    /**
     * Make sure if the String isn't JSON a GenieException is thrown.
     *
     * @throws GenieException on problem
     */
    @Test(expected = GenieServerException.class)
    public void cantGetApplicationsFromBadString() throws GenieException {
        this.entity.setApplications(UUID.randomUUID().toString());
        this.entity.getApplicationsAsList();
    }

    /**
     * Make can get applications from List.
     *
     * @throws GenieException on problem
     */
    @Test
    public void canSetApplicationsList() throws GenieException {
        final List<String> applications = Lists.newArrayList(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        this.entity.setApplicationsFromList(applications);
        Assert.assertThat(this.entity.getApplicationsAsList(), Matchers.is(applications));
    }

    /**
     * Make sure can set the timeout date for the job.
     */
    @Test
    public void canSetTimeout() {
        Assert.assertThat(this.entity.getTimeout(), Matchers.is(604800));
        final int timeout = 28023423;
        this.entity.setTimeout(timeout);
        Assert.assertThat(this.entity.getTimeout(), Matchers.is(timeout));
    }

    /**
     * Test to make sure can get a valid DTO from the job request entity.
     *
     * @throws GenieException on error
     */
    @Test
    public void canGetDTO() throws GenieException {
        final JobRequestEntity requestEntity = new JobRequestEntity();
        final String id = UUID.randomUUID().toString();
        requestEntity.setId(id);
        final String name = UUID.randomUUID().toString();
        requestEntity.setName(name);
        final String user = UUID.randomUUID().toString();
        requestEntity.setUser(user);
        final String version = UUID.randomUUID().toString();
        requestEntity.setVersion(version);
        final Date created = requestEntity.getCreated();
        final Date updated = requestEntity.getUpdated();
        final String description = UUID.randomUUID().toString();
        requestEntity.setDescription(description);
        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        requestEntity.setTags(tags);
        final String commandArgs = UUID.randomUUID().toString();
        requestEntity.setCommandArgs(commandArgs);

        final List<ClusterCriteria> clusterCriteriaList = Lists.newArrayList(
            new ClusterCriteria(
                Sets.newHashSet(
                    "one",
                    "two",
                    "three"
                )
            ),
            new ClusterCriteria(
                Sets.newHashSet(
                    "four",
                    "five",
                    "six"
                )
            ),
            new ClusterCriteria(
                Sets.newHashSet(
                    "seven",
                    "eight",
                    "nine"
                )
            )
        );
        requestEntity.setClusterCriteriasFromList(clusterCriteriaList);

        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        requestEntity.setCommandCriteriaFromSet(commandCriteria);

        final Set<String> fileDependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        requestEntity.setDependenciesFromSet(fileDependencies);

        requestEntity.setDisableLogArchival(true);

        final String email = UUID.randomUUID().toString();
        requestEntity.setEmail(email);

        final String group = UUID.randomUUID().toString();
        requestEntity.setGroup(group);

        final String setupFile = UUID.randomUUID().toString();
        requestEntity.setSetupFile(setupFile);

        final int cpu = 38;
        requestEntity.setCpu(cpu);

        final int memory = 3060;
        requestEntity.setMemory(memory);

        final List<String> applications = Lists.newArrayList(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        requestEntity.setApplicationsFromList(applications);

        final int timeout = 824197;
        requestEntity.setTimeout(timeout);

        final JobRequest request = requestEntity.getDTO();
        Assert.assertThat(request.getId(), Matchers.is(id));
        Assert.assertThat(request.getName(), Matchers.is(name));
        Assert.assertThat(request.getUser(), Matchers.is(user));
        Assert.assertThat(request.getVersion(), Matchers.is(version));
        Assert.assertThat(request.getDescription(), Matchers.is(description));
        Assert.assertThat(request.getCreated(), Matchers.is(created));
        Assert.assertThat(request.getUpdated(), Matchers.is(updated));
        Assert.assertThat(request.getTags(), Matchers.is(tags));
        Assert.assertThat(request.getCommandArgs(), Matchers.is(commandArgs));

        final List<ClusterCriteria> criterias = request.getClusterCriterias();
        Assert.assertThat(criterias.size(), Matchers.is(3));
        Assert.assertThat(criterias.get(0).getTags(), Matchers.containsInAnyOrder("one", "two", "three"));
        Assert.assertThat(criterias.get(1).getTags(), Matchers.containsInAnyOrder("four", "five", "six"));
        Assert.assertThat(criterias.get(2).getTags(), Matchers.containsInAnyOrder("seven", "eight", "nine"));

        Assert.assertThat(request.getCommandCriteria(), Matchers.is(commandCriteria));
        Assert.assertThat(request.getDependencies(), Matchers.is(fileDependencies));
        Assert.assertTrue(request.isDisableLogArchival());
        Assert.assertThat(request.getEmail(), Matchers.is(email));
        Assert.assertThat(request.getGroup(), Matchers.is(group));
        Assert.assertThat(request.getSetupFile(), Matchers.is(setupFile));
        Assert.assertThat(request.getCpu(), Matchers.is(cpu));
        Assert.assertThat(request.getMemory(), Matchers.is(memory));
        Assert.assertThat(request.getApplications(), Matchers.is(applications));
        Assert.assertThat(request.getTimeout(), Matchers.is(timeout));
    }
}
