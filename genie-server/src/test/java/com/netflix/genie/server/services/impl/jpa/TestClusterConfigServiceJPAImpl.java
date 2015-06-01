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
package com.netflix.genie.server.services.impl.jpa;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterStatus;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.CommandConfigService;
import com.netflix.genie.server.services.JobService;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import javax.validation.ConstraintViolationException;

/**
 * Tests for the CommandConfigServiceJPAImpl. Basically integration tests.
 *
 * @author tgianos
 */
@DatabaseSetup("cluster/init.xml")
public class TestClusterConfigServiceJPAImpl extends DBUnitTestBase {

    private static final String COMMAND_1_ID = "command1";
    private static final String COMMAND_2_ID = "command2";
    private static final String COMMAND_3_ID = "command3";
    private static final String JOB_1_ID = "job1";
    private static final String JOB_2_ID = "job2";

    private static final String CLUSTER_1_ID = "cluster1";
    private static final String CLUSTER_1_USER = "tgianos";
    private static final String CLUSTER_1_NAME = "h2prod";
    private static final String CLUSTER_1_VERSION = "2.4.0";
    private static final String CLUSTER_1_TYPE = "yarn";
    private static final ClusterStatus CLUSTER_1_STATUS = ClusterStatus.UP;

    private static final String CLUSTER_2_ID = "cluster2";
    private static final String CLUSTER_2_USER = "amsharma";
    private static final String CLUSTER_2_NAME = "h2query";
    private static final String CLUSTER_2_VERSION = "2.4.0";
    private static final String CLUSTER_2_TYPE = "yarn";
    private static final ClusterStatus CLUSTER_2_STATUS = ClusterStatus.UP;

    @Inject
    private ClusterConfigService service;

    @Inject
    private CommandConfigService commandService;

    @Inject
    private JobService jobService;

    /**
     * Test the get cluster method.
     *
     * @throws GenieException
     */
    @Test
    public void testGetCluster() throws GenieException {
        final Cluster cluster1 = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_1_ID, cluster1.getId());
        Assert.assertEquals(CLUSTER_1_NAME, cluster1.getName());
        Assert.assertEquals(CLUSTER_1_USER, cluster1.getUser());
        Assert.assertEquals(CLUSTER_1_VERSION, cluster1.getVersion());
        Assert.assertEquals(CLUSTER_1_STATUS, cluster1.getStatus());
        Assert.assertEquals(CLUSTER_1_TYPE, cluster1.getClusterType());
        Assert.assertEquals(5, cluster1.getTags().size());
        Assert.assertEquals(1, cluster1.getConfigs().size());
        Assert.assertEquals(3, cluster1.getCommands().size());

        final Cluster cluster2 = this.service.getCluster(CLUSTER_2_ID);
        Assert.assertEquals(CLUSTER_2_ID, cluster2.getId());
        Assert.assertEquals(CLUSTER_2_NAME, cluster2.getName());
        Assert.assertEquals(CLUSTER_2_USER, cluster2.getUser());
        Assert.assertEquals(CLUSTER_2_VERSION, cluster2.getVersion());
        Assert.assertEquals(CLUSTER_2_STATUS, cluster2.getStatus());
        Assert.assertEquals(CLUSTER_2_TYPE, cluster2.getClusterType());
        Assert.assertEquals(5, cluster2.getTags().size());
        Assert.assertEquals(2, cluster2.getConfigs().size());
        Assert.assertEquals(3, cluster1.getCommands().size());
    }

    /**
     * Test the get cluster method.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetClusterNull() throws GenieException {
        this.service.getCluster(null);
    }

    /**
     * Test the get cluster method.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetClusterNotExists() throws GenieException {
        this.service.getCluster(UUID.randomUUID().toString());
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByName() {
        final List<Cluster> clusters = this.service.getClusters(
                CLUSTER_2_NAME, null, null, null, null, 0, 10, true, null);
        Assert.assertEquals(1, clusters.size());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(0).getId());
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByStatuses() {
        final Set<ClusterStatus> statuses = EnumSet.noneOf(ClusterStatus.class);
        statuses.add(ClusterStatus.UP);
        final List<Cluster> clusters = this.service.getClusters(
                null, statuses, null, null, null, -1, -5000, true, null);
        Assert.assertEquals(2, clusters.size());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(1).getId());
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByTags() {
        final Set<String> tags = new HashSet<>();
        tags.add("prod");
        List<Cluster> clusters = this.service.getClusters(
                null, null, tags, null, null, 0, 10, true, null);
        Assert.assertEquals(1, clusters.size());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(0).getId());

        tags.clear();
        tags.add("hive");
        clusters = this.service.getClusters(
                null, null, tags, null, null, 0, 10, true, null);
        Assert.assertEquals(2, clusters.size());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(1).getId());

        tags.add("somethingThatWouldNeverReallyExist");
        clusters = this.service.getClusters(
                null, null, tags, null, null, 0, 10, true, null);
        Assert.assertTrue(clusters.isEmpty());

        tags.clear();
        clusters = this.service.getClusters(
                null, null, tags, null, null, 0, 10, true, null);
        Assert.assertEquals(2, clusters.size());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(1).getId());
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByMinUpdateTime() {
        final Calendar time = Calendar.getInstance();
        time.clear();
        time.set(2014, Calendar.JULY, 9, 2, 58, 59);
        final List<Cluster> clusters = this.service.getClusters(
                null, null, null, time.getTimeInMillis(), null, 0, 10, true, null);
        Assert.assertEquals(1, clusters.size());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(0).getId());
    }

    /**
     * Test the get clusters method.
     */
    @Test
    public void testGetClustersByMaxUpdateTime() {
        final Calendar time = Calendar.getInstance();
        time.clear();
        time.set(2014, Calendar.JULY, 8, 3, 0, 0);
        final List<Cluster> clusters = this.service.getClusters(
                null, null, null, null, time.getTimeInMillis(), 0, 10, true, null);
        Assert.assertEquals(1, clusters.size());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(0).getId());
    }

    /**
     * Test the get clusters method with descending sort.
     */
    @Test
    public void testGetClustersDescending() {
        //Default to order by Updated
        final List<Cluster> clusters = this.service.getClusters(null, null, null, null, null, 0, 10, true, null);
        Assert.assertEquals(2, clusters.size());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(1).getId());
    }

    /**
     * Test the get clusters method with ascending sort.
     */
    @Test
    public void testGetClustersAscending() {
        //Default to order by Updated
        final List<Cluster> clusters = this.service.getClusters(null, null, null, null, null, 0, 10, false, null);
        Assert.assertEquals(2, clusters.size());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(0).getId());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(1).getId());
    }

    /**
     * Test the get clusters method default order by.
     */
    @Test
    public void testGetClustersOrderBysDefault() {
        //Default to order by Updated
        final List<Cluster> clusters = this.service.getClusters(null, null, null, null, null, 0, 10, true, null);
        Assert.assertEquals(2, clusters.size());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(1).getId());
    }

    /**
     * Test the get clusters method order by updated.
     */
    @Test
    public void testGetClustersOrderBysUpdated() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("updated");
        final List<Cluster> clusters = this.service.getClusters(null, null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(2, clusters.size());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(1).getId());
    }

    /**
     * Test the get clusters method order by name.
     */
    @Test
    public void testGetClustersOrderBysUser() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("user");
        final List<Cluster> clusters = this.service.getClusters(null, null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(2, clusters.size());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(0).getId());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(1).getId());
    }

    /**
     * Test the get clusters method order by an invalid field should return the order by default value (updated).
     */
    @Test
    public void testGetClustersOrderBysInvalidField() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("I'mNotAValidField");
        final List<Cluster> clusters = this.service.getClusters(null, null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(2, clusters.size());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(1).getId());
    }

    /**
     * Test the get clusters method order by a collection field should return the order by default value (updated).
     */
    @Test
    public void testGetClustersOrderBysCollectionField() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("tags");
        final List<Cluster> clusters = this.service.getClusters(null, null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(2, clusters.size());
        Assert.assertEquals(CLUSTER_2_ID, clusters.get(0).getId());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(1).getId());
    }

    /**
     * Test the choseClusterForJob function.
     *
     * @throws GenieException
     */
    @Test
    public void testChooseClusterForJob() throws GenieException {
        final List<Cluster> clusters = this.service.chooseClusterForJob(JOB_1_ID);
        Assert.assertEquals(1, clusters.size());
        Assert.assertEquals(CLUSTER_1_ID, clusters.get(0).getId());
        final Job job = this.jobService.getJob(JOB_1_ID);
        final String chosen = job.getChosenClusterCriteriaString();
        Assert.assertEquals(8, chosen.length());
        Assert.assertTrue(chosen.contains("prod"));
        Assert.assertTrue(chosen.contains("pig"));
        Assert.assertTrue(chosen.contains(","));
    }

    /**
     * Test the choseClusterForJob function.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testChooseClusterForJobNoId() throws GenieException {
        this.service.chooseClusterForJob(null);
    }

    /**
     * Test the choseClusterForJob function.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testChooseClusterForJobNoJobExists() throws GenieException {
        this.service.chooseClusterForJob(UUID.randomUUID().toString());
    }

    /**
     * Test the choseClusterForJob function.
     *
     * @throws GenieException
     */
    @Test
    public void testChooseClusterForJobNonChosen() throws GenieException {
        Assert.assertTrue(this.service.chooseClusterForJob(JOB_2_ID).isEmpty());
    }

    /**
     * Test the create method.
     *
     * @throws GenieException
     */
    @Test
    public void testCreateCluster() throws GenieException {
        final Set<String> configs = new HashSet<>();
        configs.add("a config");
        configs.add("another config");
        configs.add("yet another config");
        final Cluster cluster = new Cluster(
                CLUSTER_1_NAME,
                CLUSTER_1_USER,
                CLUSTER_1_VERSION,
                ClusterStatus.OUT_OF_SERVICE,
                CLUSTER_1_TYPE
        );
        cluster.setConfigs(configs);
        final String id = UUID.randomUUID().toString();
        cluster.setId(id);
        final Cluster created = this.service.createCluster(cluster);
        Assert.assertNotNull(this.service.getCluster(id));
        Assert.assertEquals(id, created.getId());
        Assert.assertEquals(CLUSTER_1_NAME, created.getName());
        Assert.assertEquals(CLUSTER_1_USER, created.getUser());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, created.getStatus());
        Assert.assertEquals(CLUSTER_1_TYPE, created.getClusterType());
        Assert.assertEquals(3, created.getConfigs().size());
        this.service.deleteCluster(id);
        try {
            this.service.getCluster(id);
            Assert.fail("Should have thrown exception");
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    ge.getErrorCode()
            );
        }
    }

    /**
     * Test the create method when no id is entered.
     *
     * @throws GenieException
     */
    @Test
    public void testCreateClusterNoId() throws GenieException {
        final Set<String> configs = new HashSet<>();
        configs.add("a config");
        configs.add("another config");
        configs.add("yet another config");
        final Cluster cluster = new Cluster(
                CLUSTER_1_NAME,
                CLUSTER_1_USER,
                CLUSTER_1_VERSION,
                ClusterStatus.OUT_OF_SERVICE,
                CLUSTER_1_TYPE
        );
        cluster.setConfigs(configs);
        final Cluster created = this.service.createCluster(cluster);
        Assert.assertNotNull(created.getId());
        Assert.assertEquals(CLUSTER_1_NAME, created.getName());
        Assert.assertEquals(CLUSTER_1_USER, created.getUser());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, created.getStatus());
        Assert.assertEquals(CLUSTER_1_TYPE, created.getClusterType());
        Assert.assertEquals(3, created.getConfigs().size());
        this.service.deleteCluster(created.getId());
        try {
            this.service.getCluster(created.getId());
            Assert.fail("Should have thrown exception");
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    ge.getErrorCode()
            );
        }
    }

    /**
     * Test to make sure an exception is thrown when null is entered.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testCreateClusterNull() throws GenieException {
        this.service.createCluster(null);
    }

    /**
     * Test to make sure an exception is thrown when cluster already exists.
     *
     * @throws GenieException
     */
    @Test(expected = GenieConflictException.class)
    public void testCreateClusterAlreadyExists() throws GenieException {
        final Set<String> configs = new HashSet<>();
        configs.add("a config");
        configs.add("another config");
        configs.add("yet another config");
        final Cluster cluster = new Cluster(
                CLUSTER_1_NAME,
                CLUSTER_1_USER,
                CLUSTER_1_VERSION,
                ClusterStatus.OUT_OF_SERVICE,
                CLUSTER_1_TYPE
        );
        cluster.setConfigs(configs);
        cluster.setId(CLUSTER_1_ID);
        this.service.createCluster(cluster);
    }

    /**
     * Test to update an cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateClusterNoId() throws GenieException {
        final Cluster init = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_1_USER, init.getUser());
        Assert.assertEquals(ClusterStatus.UP, init.getStatus());
        Assert.assertEquals(5, init.getTags().size());

        final Cluster updateCluster = new Cluster();
        updateCluster.setStatus(ClusterStatus.OUT_OF_SERVICE);
        updateCluster.setUser(CLUSTER_2_USER);
        final Set<String> tags = new HashSet<>();
        tags.add("prod");
        tags.add("tez");
        tags.add("yarn");
        tags.add("hadoop");
        updateCluster.setTags(tags);
        this.service.updateCluster(CLUSTER_1_ID, updateCluster);

        final Cluster updated = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_2_USER, updated.getUser());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, updated.getStatus());
        Assert.assertEquals(6, updated.getTags().size());
    }

    /**
     * Test to update an cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateClusterWithId() throws GenieException {
        final Cluster init = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_1_USER, init.getUser());
        Assert.assertEquals(ClusterStatus.UP, init.getStatus());
        Assert.assertEquals(5, init.getTags().size());

        final Cluster updateApp = new Cluster();
        updateApp.setId(CLUSTER_1_ID);
        updateApp.setStatus(ClusterStatus.OUT_OF_SERVICE);
        updateApp.setUser(CLUSTER_2_USER);
        final Set<String> tags = new HashSet<>();
        tags.add("prod");
        tags.add("tez");
        tags.add("yarn");
        tags.add("hadoop");
        updateApp.setTags(tags);
        this.service.updateCluster(CLUSTER_1_ID, updateApp);

        final Cluster updated = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_2_USER, updated.getUser());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, updated.getStatus());
        Assert.assertEquals(6, updated.getTags().size());
    }

    /**
     * Test to update an cluster with invalid content. Should throw ConstraintViolationException from JPA layer.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateClusterWithInvalidCluster() throws GenieException {
        final Cluster init = this.service.getCluster(CLUSTER_1_ID);
        Assert.assertEquals(CLUSTER_1_USER, init.getUser());
        Assert.assertEquals(ClusterStatus.UP, init.getStatus());
        Assert.assertEquals(5, init.getTags().size());

        final Cluster updateApp = new Cluster();
        updateApp.setId(CLUSTER_1_ID);
        updateApp.setStatus(ClusterStatus.OUT_OF_SERVICE);
        updateApp.setName("");
        this.service.updateCluster(CLUSTER_1_ID, updateApp);
    }

    /**
     * Test to make sure setting the created and updated outside the system control doesn't change record in database.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateCreateAndUpdate() throws GenieException {
        final Cluster init = this.service.getCluster(CLUSTER_1_ID);
        final Date created = init.getCreated();
        final Date updated = init.getUpdated();

        init.setCreated(new Date());
        final Date zero = new Date(0);
        init.setUpdated(zero);

        final Cluster updatedCluster = this.service.updateCluster(CLUSTER_1_ID, init);
        Assert.assertEquals(created, updatedCluster.getCreated());
        Assert.assertNotEquals(updated, updatedCluster.getUpdated());
        Assert.assertNotEquals(zero, updatedCluster.getUpdated());
    }

    /**
     * Test to update an cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateClusterNullId() throws GenieException {
        this.service.updateCluster(null, new Cluster());
    }

    /**
     * Test to update an cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateClusterNullUpdateCluster() throws GenieException {
        this.service.updateCluster(CLUSTER_1_ID, null);
    }

    /**
     * Test to update an cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateClusterNoClusterExists() throws GenieException {
        this.service.updateCluster(
                UUID.randomUUID().toString(), new Cluster());
    }

    /**
     * Test to update an cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieBadRequestException.class)
    public void testUpdateClusterIdsDontMatch() throws GenieException {
        final Cluster updateApp = new Cluster();
        updateApp.setId(UUID.randomUUID().toString());
        this.service.updateCluster(CLUSTER_1_ID, updateApp);
    }

    /**
     * Test delete all.
     *
     * @throws GenieException
     */
    @Test
    public void testDeleteAll() throws GenieException {
        Assert.assertEquals(2,
                this.service.getClusters(null, null, null, null, null, 0, 10, true, null)
                        .size());
        Assert.assertEquals(2, this.service.deleteAllClusters().size());
        Assert.assertTrue(
                this.service.getClusters(null, null, null, null, null, 0, 10, true, null)
                        .isEmpty());
    }

    /**
     * Test delete.
     *
     * @throws GenieException
     */
    @Test
    public void testDelete() throws GenieException {
        Assert.assertEquals(2,
                this.commandService.getClustersForCommand(COMMAND_1_ID, null).size());
        Assert.assertEquals(2,
                this.commandService.getClustersForCommand(COMMAND_2_ID, null).size());
        Assert.assertEquals(2,
                this.commandService.getClustersForCommand(COMMAND_3_ID, null).size());

        Assert.assertEquals(CLUSTER_1_ID,
                this.service.deleteCluster(CLUSTER_1_ID).getId());

        Assert.assertEquals(1,
                this.commandService.getClustersForCommand(COMMAND_1_ID, null).size());
        Assert.assertEquals(CLUSTER_2_ID,
                this.commandService.getClustersForCommand(COMMAND_1_ID, null)
                        .iterator().next().getId());
        Assert.assertEquals(1,
                this.commandService.getClustersForCommand(COMMAND_2_ID, null).size());
        Assert.assertEquals(CLUSTER_2_ID,
                this.commandService.getClustersForCommand(COMMAND_2_ID, null)
                        .iterator().next().getId());
        Assert.assertEquals(1,
                this.commandService.getClustersForCommand(COMMAND_3_ID, null).size());
        Assert.assertEquals(CLUSTER_2_ID,
                this.commandService.getClustersForCommand(COMMAND_3_ID, null)
                        .iterator().next().getId());
    }

    /**
     * Test delete.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testDeleteNoId() throws GenieException {
        this.service.deleteCluster(null);
    }

    /**
     * Test delete.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testDeleteNoClusterToDelete() throws GenieException {
        this.service.deleteCluster(UUID.randomUUID().toString());
    }

    /**
     * Test add configurations to cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testAddConfigsToCluster() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = new HashSet<>();
        newConfigs.add(newConfig1);
        newConfigs.add(newConfig2);
        newConfigs.add(newConfig3);

        Assert.assertEquals(1,
                this.service.getConfigsForCluster(CLUSTER_1_ID).size());
        final Set<String> finalConfigs
                = this.service.addConfigsForCluster(CLUSTER_1_ID, newConfigs);
        Assert.assertEquals(4, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test add configurations to cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddConfigsToClusterNoId() throws GenieException {
        this.service.addConfigsForCluster(null, new HashSet<String>());
    }

    /**
     * Test add configurations to cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddConfigsToClusterNoConfigs() throws GenieException {
        this.service.addConfigsForCluster(CLUSTER_1_ID, null);
    }

    /**
     * Test add configurations to cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddConfigsToClusterNoCluster() throws GenieException {
        final Set<String> configs = new HashSet<>();
        configs.add(UUID.randomUUID().toString());
        this.service.addConfigsForCluster(UUID.randomUUID().toString(), configs);
    }

    /**
     * Test update configurations for cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateConfigsForCluster() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = new HashSet<>();
        newConfigs.add(newConfig1);
        newConfigs.add(newConfig2);
        newConfigs.add(newConfig3);

        Assert.assertEquals(1,
                this.service.getConfigsForCluster(CLUSTER_1_ID).size());
        final Set<String> finalConfigs
                = this.service.updateConfigsForCluster(CLUSTER_1_ID, newConfigs);
        Assert.assertEquals(3, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test update configurations for cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateConfigsForClusterNoId() throws GenieException {
        this.service.updateConfigsForCluster(null, new HashSet<String>());
    }

    /**
     * Test update configurations for cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateConfigsForClusterNoCluster() throws GenieException {
        final Set<String> configs = new HashSet<>();
        configs.add(UUID.randomUUID().toString());
        this.service.updateConfigsForCluster(UUID.randomUUID().toString(), configs);
    }

    /**
     * Test get configurations for cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testGetConfigsForCluster() throws GenieException {
        Assert.assertEquals(1,
                this.service.getConfigsForCluster(CLUSTER_1_ID).size());
    }

    /**
     * Test get configurations to cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetConfigsForClusterNoId() throws GenieException {
        this.service.getConfigsForCluster(null);
    }

    /**
     * Test get configurations to cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetConfigsForClusterNoCluster() throws GenieException {
        this.service.getConfigsForCluster(UUID.randomUUID().toString());
    }

    /**
     * Test adding commands to the cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testAddCommandsForCluster() throws GenieException {
        final Command command1 = this.commandService.createCommand(
                new Command(
                        "name",
                        "user",
                        "23.1.0",
                        CommandStatus.ACTIVE,
                        "pig"
                )
        );
        final Command command2 = this.commandService.createCommand(
                new Command(
                        "name2",
                        "user2",
                        "23.1.1",
                        CommandStatus.INACTIVE,
                        "pig2"
                )
        );
        final List<Command> newCommands = new ArrayList<>();
        newCommands.add(command1);
        newCommands.add(command2);
        Assert.assertEquals(
                3,
                this.service.getCommandsForCluster(CLUSTER_1_ID, null).size()
        );
        final List<Command> commands
                = this.service.addCommandsForCluster(CLUSTER_1_ID, newCommands);
        Assert.assertEquals(
                5,
                commands.size()
        );
    }

    /**
     * Test adding commands to the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddCommandsForClusterNoId() throws GenieException {
        this.service.addCommandsForCluster(null, new ArrayList<Command>());
    }

    /**
     * Test adding commands to the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddCommandsForClusterNoCommands() throws GenieException {
        this.service.addCommandsForCluster(UUID.randomUUID().toString(), null);
    }

    /**
     * Test adding commands to the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddCommandsForClusterClusterDoesntExist() throws GenieException {
        final List<Command> commands = new ArrayList<>();
        commands.add(new Command());
        this.service.addCommandsForCluster(UUID.randomUUID().toString(), commands);
    }

    /**
     * Test adding commands to the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddCommandsForClusterCommandDoesntExist()
            throws GenieException {
        final List<Command> commands = new ArrayList<>();
        final Command command = new Command();
        command.setId(UUID.randomUUID().toString());
        commands.add(command);
        this.service.addCommandsForCluster(
                CLUSTER_1_ID, commands);
    }

    /**
     * Test the Get commands for cluster function.
     *
     * @throws GenieException
     */
    @Test
    public void testGetCommandsForCluster() throws GenieException {
        final List<Command> commands
                = this.service.getCommandsForCluster(CLUSTER_1_ID, null);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(COMMAND_1_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId());
        Assert.assertEquals(COMMAND_2_ID, commands.get(2).getId());
    }

    /**
     * Test the Get clusters for cluster function.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetCommandsForClusterNoId() throws GenieException {
        this.service.getCommandsForCluster("", null);
    }

    /**
     * Test the Get clusters for cluster function.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetCommandsForClusterNoCluster() throws GenieException {
        this.service.getCommandsForCluster(UUID.randomUUID().toString(), null);
    }

    /**
     * Test updating commands for the cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateCommandsForCluster() throws GenieException {
        final Command command1 = this.commandService.createCommand(
                new Command(
                        "name",
                        "user",
                        "23.1.0",
                        CommandStatus.ACTIVE,
                        "pig"
                )
        );
        final Command command2 = this.commandService.createCommand(
                new Command(
                        "name2",
                        "user2",
                        "23.1.1",
                        CommandStatus.INACTIVE,
                        "pig2"
                )
        );
        final List<Command> newCommands = new ArrayList<>();
        newCommands.add(command1);
        newCommands.add(command2);
        Assert.assertEquals(
                3,
                this.service.getCommandsForCluster(CLUSTER_1_ID, null).size()
        );
        final List<Command> commands
                = this.service.updateCommandsForCluster(
                CLUSTER_1_ID,
                newCommands
        );
        Assert.assertEquals(
                2,
                commands.size()
        );
    }

    /**
     * Test updating commands for the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateCommandsForClusterNoId() throws GenieException {
        this.service.updateCommandsForCluster(null, new ArrayList<Command>());
    }

    /**
     * Test updating commands for the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateCommandsForClusterNoCommands() throws GenieException {
        this.service.updateCommandsForCluster(UUID.randomUUID().toString(), null);
    }

    /**
     * Test updating commands for the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateCommandsForClusterClusterDoesntExist() throws GenieException {
        final List<Command> commands = new ArrayList<>();
        commands.add(new Command());
        this.service.updateCommandsForCluster(UUID.randomUUID().toString(), commands);
    }

    /**
     * Test updating commands for the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateCommandsForClusterCommandDoesntExist()
            throws GenieException {
        final List<Command> commands = new ArrayList<>();
        final Command command = new Command();
        command.setId(UUID.randomUUID().toString());
        commands.add(command);
        this.service.updateCommandsForCluster(
                CLUSTER_1_ID, commands);
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveAllCommandsForCluster() throws GenieException {
        Assert.assertEquals(
                3,
                this.service.getCommandsForCluster(CLUSTER_1_ID, null).size()
        );
        Assert.assertEquals(
                0,
                this.service.removeAllCommandsForCluster(CLUSTER_1_ID).size()
        );
        Assert.assertEquals(
                0,
                this.service.getCommandsForCluster(CLUSTER_1_ID, null).size()
        );
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAllCommandsForClusterNoId() throws GenieException {
        this.service.removeAllCommandsForCluster(null);
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllCommandsForClusterNoCluster()
            throws GenieException {
        this.service.removeAllCommandsForCluster(UUID.randomUUID().toString());
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveCommandForCluster() throws GenieException {
        Assert.assertEquals(
                3,
                this.service.getCommandsForCluster(CLUSTER_1_ID, null).size()
        );
        Assert.assertEquals(
                2,
                this.service.removeCommandForCluster(
                        CLUSTER_1_ID,
                        COMMAND_1_ID
                ).size()
        );
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveCommandForClusterNoId() throws GenieException {
        this.service.removeCommandForCluster(null, COMMAND_1_ID);
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveCommandForClusterNoCmdId() throws GenieException {
        this.service.removeCommandForCluster(CLUSTER_1_ID, null);
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveCommandForClusterNoCluster() throws GenieException {
        this.service.removeCommandForCluster(
                UUID.randomUUID().toString(), COMMAND_1_ID);
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveCommandForClusterNoCommand() throws GenieException {
        this.service.removeCommandForCluster(
                CLUSTER_1_ID, UUID.randomUUID().toString());
    }

    /**
     * Test add tags to cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testAddTagsToCluster() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = new HashSet<>();
        newTags.add(newTag1);
        newTags.add(newTag2);
        newTags.add(newTag3);

        Assert.assertEquals(5,
                this.service.getTagsForCluster(CLUSTER_1_ID).size());
        final Set<String> finalTags
                = this.service.addTagsForCluster(CLUSTER_1_ID, newTags);
        Assert.assertEquals(8, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test add tags to cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddTagsToClusterNoId() throws GenieException {
        this.service.addTagsForCluster(null, new HashSet<String>());
    }

    /**
     * Test add tags to cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddTagsToClusterNoTags() throws GenieException {
        this.service.addTagsForCluster(CLUSTER_1_ID, null);
    }

    /**
     * Test add tags to cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddTagsForClusterNoCluster() throws GenieException {
        final Set<String> tags = new HashSet<>();
        tags.add(UUID.randomUUID().toString());
        this.service.addTagsForCluster(UUID.randomUUID().toString(), tags);
    }

    /**
     * Test update tags for cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateTagsForCluster() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = new HashSet<>();
        newTags.add(newTag1);
        newTags.add(newTag2);
        newTags.add(newTag3);

        Assert.assertEquals(5,
                this.service.getTagsForCluster(CLUSTER_1_ID).size());
        final Set<String> finalTags
                = this.service.updateTagsForCluster(CLUSTER_1_ID, newTags);
        Assert.assertEquals(5, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test update tags for cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateTagsForClusterNoId() throws GenieException {
        this.service.updateTagsForCluster(null, new HashSet<String>());
    }

    /**
     * Test update tags for cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateTagsForClusterNoApp() throws GenieException {
        final Set<String> tags = new HashSet<>();
        tags.add(UUID.randomUUID().toString());
        this.service.updateTagsForCluster(UUID.randomUUID().toString(), tags);
    }

    /**
     * Test get tags for cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testGetTagsForCluster() throws GenieException {
        Assert.assertEquals(5,
                this.service.getTagsForCluster(CLUSTER_1_ID).size());
    }

    /**
     * Test get tags to cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetTagsForClusterNoId() throws GenieException {
        this.service.getTagsForCluster(null);
    }

    /**
     * Test get tags to cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetTagsForClusterNoCluster() throws GenieException {
        this.service.getTagsForCluster(UUID.randomUUID().toString());
    }

    /**
     * Test remove all tags for cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveAllTagsForCluster() throws GenieException {
        Assert.assertEquals(5,
                this.service.getTagsForCluster(CLUSTER_1_ID).size());
        final Set<String> finalTags
                = this.service.removeAllTagsForCluster(CLUSTER_1_ID);
        Assert.assertEquals(2,
                finalTags.size());
    }

    /**
     * Test remove all tags for cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAllTagsForClusterNoId() throws GenieException {
        this.service.removeAllTagsForCluster(null);
    }

    /**
     * Test remove all tags for cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllTagsForClusterNoCluster() throws GenieException {
        this.service.removeAllTagsForCluster(UUID.randomUUID().toString());
    }

    /**
     * Test remove tag for cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveTagForCluster() throws GenieException {
        final Set<String> tags
                = this.service.getTagsForCluster(CLUSTER_1_ID);
        Assert.assertEquals(5, tags.size());
        Assert.assertEquals(4,
                this.service.removeTagForCluster(
                        CLUSTER_1_ID,
                        "prod").size()
        );
    }

    /**
     * Test remove tag for cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveTagForClusterNullTag() throws GenieException {
        this.service.removeTagForCluster(CLUSTER_1_ID, null);
    }

    /**
     * Test remove configuration for cluster.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveTagForClusterNoId() throws GenieException {
        this.service.removeTagForCluster(null, "something");
    }

    /**
     * Test remove configuration for cluster.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveTagForClusterNoCluster() throws GenieException {
        this.service.removeTagForCluster(
                UUID.randomUUID().toString(),
                "something"
        );
    }
}
