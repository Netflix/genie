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
package com.netflix.genie.core.jpa.services;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for the ClusterServiceJPAImpl.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Category(UnitTest.class)
public class JpaClusterServiceImplUnitTests {

    private static final String CLUSTER_1_ID = "cluster1";
    private static final String CLUSTER_1_USER = "tgianos";
    private static final String CLUSTER_1_NAME = "h2prod";
    private static final String CLUSTER_1_VERSION = "2.4.0";

    private JpaClusterServiceImpl service;
    private JpaClusterRepository jpaClusterRepository;
    private JpaCommandRepository jpaCommandRepository;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jpaClusterRepository = Mockito.mock(JpaClusterRepository.class);
        this.jpaCommandRepository = Mockito.mock(JpaCommandRepository.class);
        this.service = new JpaClusterServiceImpl(this.jpaClusterRepository, this.jpaCommandRepository);
    }

    /**
     * Test the get cluster method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetClusterNotExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.getCluster(id);
    }

    /**
     * Test to make sure an exception is thrown when cluster already exists.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieConflictException.class)
    public void testCreateClusterAlreadyExists() throws GenieException {
        final Set<String> configs = Sets.newHashSet("a config", "another config", "yet another config");
        final Cluster cluster = new Cluster.Builder(
            CLUSTER_1_NAME,
            CLUSTER_1_USER,
            CLUSTER_1_VERSION,
            ClusterStatus.OUT_OF_SERVICE
        )
            .withId(CLUSTER_1_ID)
            .withConfigs(configs)
            .build();

        Mockito.when(this.jpaClusterRepository.exists(CLUSTER_1_ID)).thenReturn(true);
        this.service.createCluster(cluster);
    }

    /**
     * Test to update a cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateClusterNoClusterExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.updateCluster(id, new Cluster.Builder(null, null, null, null).build());
    }

    /**
     * Test to update a cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieBadRequestException.class)
    public void testUpdateClusterIdsDontMatch() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        Mockito.when(this.jpaClusterRepository.exists(id)).thenReturn(true);
        Mockito.when(cluster.getId()).thenReturn(Optional.of(UUID.randomUUID().toString()));
        this.service.updateCluster(id, cluster);
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testDeleteNoClusterToDelete() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.deleteCluster(id);
    }

    /**
     * Test add configurations to cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddConfigsToClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.addConfigsForCluster(UUID.randomUUID().toString(), Sets.newHashSet());
    }

    /**
     * Test update configurations for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateConfigsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.updateConfigsForCluster(UUID.randomUUID().toString(), Sets.newHashSet());
    }

    /**
     * Test get configurations to cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetConfigsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.getConfigsForCluster(id);
    }

    /**
     * Test add dependencies to cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddDepsToClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.addDependenciesForCluster(UUID.randomUUID().toString(), Sets.newHashSet());
    }

    /**
     * Test update dependencies of cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateDepsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.updateDependenciesForCluster(UUID.randomUUID().toString(), Sets.newHashSet());
    }

    /**
     * Test get dependencies from cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetDepsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.getDependenciesForCluster(id);
    }

    /**
     * Test remove all dependencies from cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllDepsFromClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.removeAllDependenciesForCluster(id);
    }

    /**
     * Test remove dependency from cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveDepFromClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.removeDependencyForCluster(id, "something");
    }

    /**
     * Test adding commands to the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddCommandsForClusterClusterDoesntExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.addCommandsForCluster(UUID.randomUUID().toString(), new ArrayList<>());
    }

    /**
     * Test adding commands to the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testAddCommandsForClusterCommandDoesntExist() throws GenieException {
        final List<String> commandIds = new ArrayList<>();
        final String commandId = UUID.randomUUID().toString();
        commandIds.add(commandId);
        final ClusterEntity clusterEntity = Mockito.mock(ClusterEntity.class);
        Mockito.when(this.jpaClusterRepository.findOne(Mockito.anyString())).thenReturn(clusterEntity);
        Mockito.when(this.jpaCommandRepository.findOne(commandId)).thenReturn(null);
        this.service.addCommandsForCluster(CLUSTER_1_ID, commandIds);
    }

    /**
     * Make sure we can't add duplicate commands to the cluster.
     *
     * @throws GenieException on error
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantAddDuplicateCommandsToCluster() throws GenieException {
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        final List<String> commandIds = Lists.newArrayList(commandId2);
        final ClusterEntity clusterEntity = Mockito.mock(ClusterEntity.class);
        final CommandEntity command1 = Mockito.mock(CommandEntity.class);
        Mockito.when(command1.getId()).thenReturn(commandId1);
        final CommandEntity command2 = Mockito.mock(CommandEntity.class);
        Mockito.when(command2.getId()).thenReturn(commandId2);
        Mockito.when(clusterEntity.getCommands()).thenReturn(Lists.newArrayList(command1, command2));

        Mockito.when(this.jpaClusterRepository.findOne(Mockito.anyString())).thenReturn(clusterEntity);
        Mockito.when(this.jpaCommandRepository.findOne(commandId1)).thenReturn(command1);
        Mockito.when(this.jpaCommandRepository.findOne(commandId2)).thenReturn(command2);
        this.service.addCommandsForCluster(CLUSTER_1_ID, commandIds);
    }

    /**
     * Test the Get clusters for cluster function.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetCommandsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.getCommandsForCluster(id, null);
    }
//TODO: Missing tests for statuses

    /**
     * Test updating commands for the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateCommandsForClusterClusterDoesntExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.setCommandsForCluster(id, new ArrayList<>());
    }

    /**
     * Test updating commands for the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetCommandsForClusterCommandDoesntExist() throws GenieException {
        final List<String> commandIds = new ArrayList<>();
        final String commandId = UUID.randomUUID().toString();
        commandIds.add(commandId);
        final ClusterEntity cluster
            = Mockito.mock(ClusterEntity.class);
        Mockito.when(this.jpaClusterRepository.findOne(CLUSTER_1_ID)).thenReturn(cluster);
        Mockito.when(this.jpaCommandRepository.findOne(commandId)).thenReturn(null);
        this.service.setCommandsForCluster(CLUSTER_1_ID, commandIds);
    }

    /**
     * Make sure we can't update the commands for a cluster if there are duplicate ids in the list of commandIds.
     *
     * @throws GenieException On error
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantUpdateCommandsForClusterWithDuplicates() throws GenieException {
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        final List<String> commandIds = Lists.newArrayList(commandId1, commandId2, commandId1);
        final ClusterEntity cluster = Mockito.mock(ClusterEntity.class);
        Mockito.when(this.jpaClusterRepository.findOne(CLUSTER_1_ID)).thenReturn(cluster);
        Mockito.when(this.jpaCommandRepository.findOne(commandId1)).thenReturn(Mockito.mock(CommandEntity.class));
        Mockito.when(this.jpaCommandRepository.findOne(commandId2)).thenReturn(Mockito.mock(CommandEntity.class));
        this.service.setCommandsForCluster(CLUSTER_1_ID, commandIds);
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllCommandsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.removeAllCommandsForCluster(id);
    }

    /**
     * Test removing all commands for the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveCommandForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.removeCommandForCluster(id, UUID.randomUUID().toString());
    }

    /**
     * Test removing command for the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveCommandForClusterNoCommand() throws GenieException {
        final ClusterEntity clusterEntity
            = Mockito.mock(ClusterEntity.class);
        Mockito.when(this.jpaClusterRepository.findOne(CLUSTER_1_ID)).thenReturn(clusterEntity);
        final String commandId = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findOne(commandId)).thenReturn(null);
        this.service.removeCommandForCluster(CLUSTER_1_ID, commandId);
    }

    /**
     * Test add tags to cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddTagsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.addTagsForCluster(UUID.randomUUID().toString(), Sets.newHashSet());
    }

    /**
     * Test update tags for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateTagsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.updateTagsForCluster(UUID.randomUUID().toString(), Sets.newHashSet());
    }

    /**
     * Test get tags to cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetTagsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.getTagsForCluster(id);
    }

    /**
     * Test remove all tags for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllTagsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.removeAllTagsForCluster(id);
    }

    /**
     * Test remove configuration for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveTagForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findOne(id)).thenReturn(null);
        this.service.removeTagForCluster(id, "something");
    }
}
