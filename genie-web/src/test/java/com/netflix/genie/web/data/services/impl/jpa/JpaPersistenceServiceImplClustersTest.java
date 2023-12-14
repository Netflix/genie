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

import com.google.common.collect.Sets;
import com.netflix.genie.common.internal.dtos.Cluster;
import com.netflix.genie.common.internal.dtos.ClusterMetadata;
import com.netflix.genie.common.internal.dtos.ClusterRequest;
import com.netflix.genie.common.internal.dtos.ClusterStatus;
import com.netflix.genie.common.internal.dtos.ExecutionEnvironment;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.FileEntity;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaRepositories;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.exceptions.checked.PreconditionFailedException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.dao.DuplicateKeyException;

import jakarta.persistence.EntityManager;
import java.time.Instant;
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
    private JpaFileRepository jpaFileRepository;

    @BeforeEach
    void setup() {
        this.jpaClusterRepository = Mockito.mock(JpaClusterRepository.class);
        this.jpaFileRepository = Mockito.mock(JpaFileRepository.class);
        final JpaRepositories jpaRepositories = Mockito.mock(JpaRepositories.class);
        Mockito.when(jpaRepositories.getClusterRepository()).thenReturn(this.jpaClusterRepository);
        Mockito.when(jpaRepositories.getFileRepository()).thenReturn(this.jpaFileRepository);
        this.service = new JpaPersistenceServiceImpl(
            Mockito.mock(EntityManager.class),
            jpaRepositories,
            Mockito.mock(BraveTracingComponents.class)
        );
    }

    @Test
    void getClusterNotExists() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getCluster(id));
    }

    @Test
    void createClusterAlreadyExists() {
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
    void updateClusterNoClusterExists() {
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
    void updateClusterIdsDontMatch() {
        final String id = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        Mockito.when(this.jpaClusterRepository.existsByUniqueId(id)).thenReturn(true);
        Mockito.when(cluster.getId()).thenReturn(UUID.randomUUID().toString());
        Assertions
            .assertThatExceptionOfType(PreconditionFailedException.class)
            .isThrownBy(() -> this.service.updateCluster(id, cluster));
    }

    @Test
    void addConfigsToClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.addConfigsToResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void updateConfigsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.updateConfigsForResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void getConfigsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getConfigsForResource(id, Cluster.class));
    }

    @Test
    void addDepsToClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.addDependenciesToResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void updateDepsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.updateDependenciesForResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void getDepsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getDependenciesForResource(id, Cluster.class));
    }

    @Test
    void removeAllDepsFromClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeAllDependenciesForResource(id, Cluster.class));
    }

    @Test
    void removeDepFromClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeDependencyForResource(id, "something", Cluster.class));
    }

    @Test
    void addTagsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.addTagsToResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void updateTagsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.updateTagsForResource(id, Sets.newHashSet(), Cluster.class));
    }

    @Test
    void getTagsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getTagsForResource(id, Cluster.class));
    }

    @Test
    void removeAllTagsForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeAllTagsForResource(id, Cluster.class));
    }

    @Test
    void removeTagForClusterNoCluster() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaClusterRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.removeTagForResource(id, "something", Cluster.class));
    }
}
