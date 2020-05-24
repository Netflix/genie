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
package com.netflix.genie.web.data.services.impl.jpa;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.ClusterMetadata;
import com.netflix.genie.common.external.dtos.v4.ClusterRequest;
import com.netflix.genie.common.external.dtos.v4.ClusterStatus;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.FileEntity;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.ClusterCommandsProjection;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaRepositories;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.exceptions.checked.PreconditionFailedException;
import com.netflix.genie.web.services.AttachmentService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.dao.DuplicateKeyException;

import javax.persistence.EntityManager;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for the {@link JpaPersistenceServiceImpl} focusing on cluster functionality.
 *
 * @author tgianos
 * @since 2.0.0
 */
class JpaPersistenceServiceImplClustersTest {

    private static final String CLUSTER_1_ID = "cluster1";
    private static final String CLUSTER_1_USER = "tgianos";
    private static final String CLUSTER_1_NAME = "h2prod";
    private static final String CLUSTER_1_VERSION = "2.4.0";

    private JpaPersistenceServiceImpl service;
    private JpaClusterRepository jpaClusterRepository;
    private JpaCommandRepository jpaCommandRepository;
    private JpaFileRepository jpaFileRepository;

    @BeforeEach
    void setup() {
        this.jpaClusterRepository = Mockito.mock(JpaClusterRepository.class);
        this.jpaCommandRepository = Mockito.mock(JpaCommandRepository.class);
        this.jpaFileRepository = Mockito.mock(JpaFileRepository.class);
        final JpaRepositories jpaRepositories = Mockito.mock(JpaRepositories.class);
        Mockito.when(jpaRepositories.getClusterRepository()).thenReturn(this.jpaClusterRepository);
        Mockito.when(jpaRepositories.getCommandRepository()).thenReturn(this.jpaCommandRepository);
        Mockito.when(jpaRepositories.getFileRepository()).thenReturn(this.jpaFileRepository);
        this.service = new JpaPersistenceServiceImpl(
            Mockito.mock(EntityManager.class),
            jpaRepositories,
            Mockito.mock(AttachmentService.class)
        );
    }

    @Test
    void testGetClusterNotExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getCluster(id));
    }

    @Test
    void testCreateClusterAlreadyExists() {
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
            .when(this.jpaFileRepository.findByFile(Mockito.anyString()))
            .thenReturn(Optional.of(new FileEntity(UUID.randomUUID().toString())));
        Mockito
            .when(this.jpaClusterRepository.save(Mockito.any(ClusterEntity.class)))
            .thenThrow(new DuplicateKeyException("Duplicate Key"));
        Assertions
            .assertThatExceptionOfType(IdAlreadyExistsException.class)
            .isThrownBy(() -> this.service.saveCluster(request));
    }

    @Test
    void testUpdateClusterNoClusterExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(
                () -> this.service.updateCluster(
                    id,
                    new Cluster(
                        id,
                        Instant.now(),
                        Instant.now(),
                        new ExecutionEnvironment(null, null, null),
                        new ClusterMetadata.Builder(" ", " ", " ", ClusterStatus.UP).build()
                    )
                )
            );
    }

    @Test
    void testUpdateClusterIdsDontMatch() {
        final String id = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        Mockito.when(this.jpaClusterRepository.existsByUniqueId(id)).thenReturn(true);
        Mockito.when(cluster.getId()).thenReturn(UUID.randomUUID().toString());
        Assertions
            .assertThatExceptionOfType(PreconditionFailedException.class)
            .isThrownBy(() -> this.service.updateCluster(id, cluster));
    }

    @Test
    void testAddConfigsToClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.addConfigsToResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void testUpdateConfigsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.updateConfigsForResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void testGetConfigsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getConfigsForResource(id, Cluster.class));
    }

    @Test
    void testAddDepsToClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.addDependenciesToResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void testUpdateDepsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.updateDependenciesForResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void testGetDepsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getDependenciesForResource(id, Cluster.class));
    }

    @Test
    void testRemoveAllDepsFromClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeAllDependenciesForResource(id, Cluster.class));
    }

    @Test
    void testRemoveDepFromClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeDependencyForResource(id, "something", Cluster.class));
    }

    @Test
    void testAddCommandsForClusterClusterDoesntExist() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.addCommandsForCluster(id, new ArrayList<>()));
    }

    @Test
    void testAddCommandsForClusterCommandDoesntExist() {
        final List<String> commandIds = new ArrayList<>();
        final String commandId = UUID.randomUUID().toString();
        commandIds.add(commandId);
        final ClusterEntity clusterEntity = Mockito.mock(ClusterEntity.class);
        Mockito
            .when(this.jpaClusterRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.of(clusterEntity));
        Mockito
            .when(this.jpaCommandRepository.findByUniqueId(commandId))
            .thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.service.addCommandsForCluster(CLUSTER_1_ID, commandIds));
    }

    @Test
    void cantAddDuplicateCommandsToCluster() {
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        final List<String> commandIds = Lists.newArrayList(commandId2);
        final ClusterEntity clusterEntity = Mockito.mock(ClusterEntity.class);
        final CommandEntity command1 = Mockito.mock(CommandEntity.class);
        Mockito.when(command1.getUniqueId()).thenReturn(commandId1);
        final CommandEntity command2 = Mockito.mock(CommandEntity.class);
        Mockito.when(command2.getUniqueId()).thenReturn(commandId2);
        Mockito.when(clusterEntity.getCommands()).thenReturn(Lists.newArrayList(command1, command2));

        Mockito
            .when(this.jpaClusterRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.of(clusterEntity));
        Mockito.when(this.jpaCommandRepository.findByUniqueId(commandId1)).thenReturn(Optional.of(command1));
        Mockito.when(this.jpaCommandRepository.findByUniqueId(commandId2)).thenReturn(Optional.of(command2));
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.service.addCommandsForCluster(CLUSTER_1_ID, commandIds));
    }

    @Test
    void testGetCommandsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito
            .when(this.jpaClusterRepository.findByUniqueId(id, ClusterCommandsProjection.class))
            .thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getCommandsForCluster(id, null));
    }

    @Test
    void testUpdateCommandsForClusterClusterDoesntExist() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.setCommandsForCluster(id, new ArrayList<>()));
    }

    @Test
    void testSetCommandsForClusterCommandDoesntExist() {
        final List<String> commandIds = new ArrayList<>();
        final String commandId = UUID.randomUUID().toString();
        commandIds.add(commandId);
        final ClusterEntity cluster
            = Mockito.mock(ClusterEntity.class);
        Mockito.when(this.jpaClusterRepository.findByUniqueId(CLUSTER_1_ID)).thenReturn(Optional.of(cluster));
        Mockito.when(this.jpaCommandRepository.findByUniqueId(commandId)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.service.setCommandsForCluster(CLUSTER_1_ID, commandIds));
    }

    @Test
    void cantUpdateCommandsForClusterWithDuplicates() {
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
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.service.setCommandsForCluster(CLUSTER_1_ID, commandIds));
    }

    @Test
    void testRemoveAllCommandsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.removeAllCommandsForCluster(id));
    }

    @Test
    void testRemoveCommandForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.removeCommandForCluster(id, UUID.randomUUID().toString()));
    }

    @Test
    void testRemoveCommandForClusterNoCommand() {
        final ClusterEntity clusterEntity
            = Mockito.mock(ClusterEntity.class);
        Mockito.when(this.jpaClusterRepository.findByUniqueId(CLUSTER_1_ID)).thenReturn(Optional.of(clusterEntity));
        final String commandId = UUID.randomUUID().toString();
        Mockito.when(this.jpaCommandRepository.findByUniqueId(commandId)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.removeCommandForCluster(CLUSTER_1_ID, commandId));
    }

    @Test
    void testAddTagsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.addTagsToResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void testUpdateTagsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.updateTagsForResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void testGetTagsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getTagsForResource(id, Cluster.class));
    }

    @Test
    void testRemoveAllTagsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeAllTagsForResource(id, Cluster.class));
    }

    @Test
    void testRemoveTagForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeTagForResource(id, "something", Cluster.class));
    }
}
