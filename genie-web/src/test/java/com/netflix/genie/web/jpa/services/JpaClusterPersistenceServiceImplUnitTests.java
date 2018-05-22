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
package com.netflix.genie.web.jpa.services;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.ClusterMetadata;
import com.netflix.genie.common.internal.dto.v4.ClusterRequest;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.jpa.entities.ClusterEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.entities.projections.ClusterCommandsProjection;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.dao.DuplicateKeyException;

import java.time.Instant;
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
public class JpaClusterPersistenceServiceImplUnitTests {

    private static final String CLUSTER_1_ID = "cluster1";
    private static final String CLUSTER_1_USER = "tgianos";
    private static final String CLUSTER_1_NAME = "h2prod";
    private static final String CLUSTER_1_VERSION = "2.4.0";

    private JpaClusterPersistenceServiceImpl service;
    private JpaClusterRepository jpaClusterRepository;
    private JpaCommandRepository jpaCommandRepository;
    private JpaFilePersistenceService filePersistenceService;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jpaClusterRepository = Mockito.mock(JpaClusterRepository.class);
        this.jpaCommandRepository = Mockito.mock(JpaCommandRepository.class);
        this.filePersistenceService = Mockito.mock(JpaFilePersistenceService.class);
        this.service = new JpaClusterPersistenceServiceImpl(
            Mockito.mock(JpaTagPersistenceService.class),
            this.filePersistenceService,
            Mockito.mock(JpaApplicationRepository.class),
            this.jpaClusterRepository,
            this.jpaCommandRepository
        );
    }

    /**
     * Test the get cluster method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetClusterNotExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
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
        final ClusterRequest request = new ClusterRequest.Builder(
            new ClusterMetadata.Builder(
                CLUSTER_1_NAME,
                CLUSTER_1_USER,
                CLUSTER_1_VERSION,
                ClusterStatus.OUT_OF_SERVICE
            )
                .build()
        )
            .withRequestedId(CLUSTER_1_ID)
            .withResources(new ExecutionEnvironment(configs, null, null))
            .build();

        Mockito
            .when(this.filePersistenceService.getFile(Mockito.anyString()))
            .thenReturn(Optional.of(new FileEntity(UUID.randomUUID().toString())));
        Mockito
            .when(this.jpaClusterRepository.save(Mockito.any(ClusterEntity.class)))
            .thenThrow(new DuplicateKeyException("Duplicate Key"));
        this.service.createCluster(request);
    }

    /**
     * Test to update a cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateClusterNoClusterExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.updateCluster(
            id,
            new Cluster(
                id,
                Instant.now(),
                Instant.now(),
                new ExecutionEnvironment(null, null, null),
                new ClusterMetadata.Builder(" ", " ", " ", ClusterStatus.UP).build()
            )
        );
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
        Mockito.when(this.jpaClusterRepository.existsByUniqueId(id)).thenReturn(true);
        Mockito.when(cluster.getId()).thenReturn(UUID.randomUUID().toString());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.addConfigsForCluster(id, Sets.newHashSet());
    }

    /**
     * Test update configurations for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateConfigsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.updateConfigsForCluster(id, Sets.newHashSet());
    }

    /**
     * Test get configurations to cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetConfigsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.addDependenciesForCluster(id, Sets.newHashSet());
    }

    /**
     * Test update dependencies of cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateDepsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.updateDependenciesForCluster(id, Sets.newHashSet());
    }

    /**
     * Test get dependencies from cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetDepsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.addCommandsForCluster(id, new ArrayList<>());
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
        Mockito.when(
            this.jpaClusterRepository.findByUniqueId(Mockito.anyString())).thenReturn(Optional.of(clusterEntity)
        );
        Mockito.when(this.jpaCommandRepository.findByUniqueId(commandId)).thenReturn(Optional.empty());
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
        Mockito.when(command1.getUniqueId()).thenReturn(commandId1);
        final CommandEntity command2 = Mockito.mock(CommandEntity.class);
        Mockito.when(command2.getUniqueId()).thenReturn(commandId2);
        Mockito.when(clusterEntity.getCommands()).thenReturn(Lists.newArrayList(command1, command2));

        Mockito.when(
            this.jpaClusterRepository.findByUniqueId(Mockito.anyString())).thenReturn(Optional.of(clusterEntity)
        );
        Mockito.when(this.jpaCommandRepository.findByUniqueId(commandId1)).thenReturn(Optional.of(command1));
        Mockito.when(this.jpaCommandRepository.findByUniqueId(commandId2)).thenReturn(Optional.of(command2));
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
        Mockito
            .when(this.jpaClusterRepository.findByUniqueId(id, ClusterCommandsProjection.class))
            .thenReturn(Optional.empty());
        this.service.getCommandsForCluster(id, null);
    }

    // TODO: Missing tests for statuses

    /**
     * Test updating commands for the cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateCommandsForClusterClusterDoesntExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(CLUSTER_1_ID)).thenReturn(Optional.of(cluster));
        Mockito.when(this.jpaCommandRepository.findByUniqueId(commandId)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(CLUSTER_1_ID)).thenReturn(Optional.of(cluster));
        Mockito
            .when(this.jpaCommandRepository.findByUniqueId(commandId1))
            .thenReturn(Optional.of(Mockito.mock(CommandEntity.class)));
        Mockito
            .when(this.jpaCommandRepository.findByUniqueId(commandId2))
            .thenReturn(Optional.of(Mockito.mock(CommandEntity.class)));
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(CLUSTER_1_ID)).thenReturn(Optional.of(clusterEntity));
        final String commandId = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(commandId)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.addTagsForCluster(id, Sets.newHashSet());
    }

    /**
     * Test update tags for cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateTagsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.updateTagsForCluster(id, Sets.newHashSet());
    }

    /**
     * Test get tags to cluster.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetTagsForClusterNoCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
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
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.service.removeTagForCluster(id, "something");
    }
}
