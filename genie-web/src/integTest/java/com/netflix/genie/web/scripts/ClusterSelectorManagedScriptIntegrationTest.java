/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.scripts;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.ClusterMetadata;
import com.netflix.genie.common.external.dtos.v4.ClusterStatus;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.external.dtos.v4.JobMetadata;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.internal.util.PropertiesMapCache;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.properties.ClusterSelectorScriptProperties;
import com.netflix.genie.web.properties.ScriptManagerProperties;
import com.netflix.genie.web.selectors.ClusterSelectionContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

import javax.script.ScriptEngineManager;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class ClusterSelectorManagedScriptIntegrationTest {

    private static final ExecutionResourceCriteria CRITERIA = new ExecutionResourceCriteria(
        Lists.newArrayList(new Criterion.Builder().withId(UUID.randomUUID().toString()).build()),
        new Criterion.Builder().withName(UUID.randomUUID().toString()).build(),
        null
    );
    private static final JobMetadata JOB_METADATA = new JobMetadata.Builder(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()
    ).build();

    private static final Cluster CLUSTER_0 = createTestCluster("0");
    private static final Cluster CLUSTER_1 = createTestCluster("1");
    private static final Cluster CLUSTER_2 = createTestCluster("2");

    private static final String JOB_REQUEST_0_ID = "0";
    private static final String JOB_REQUEST_1_ID = "1";
    private static final String JOB_REQUEST_2_ID = "2";
    private static final String JOB_REQUEST_3_ID = "3";
    private static final String JOB_REQUEST_4_ID = "4";
    private static final String JOB_REQUEST_5_ID = "5";

    private static final JobRequest JOB_REQUEST_0 = createTestJobRequest(JOB_REQUEST_0_ID);
    private static final JobRequest JOB_REQUEST_1 = createTestJobRequest(JOB_REQUEST_1_ID);
    private static final JobRequest JOB_REQUEST_2 = createTestJobRequest(JOB_REQUEST_2_ID);
    private static final JobRequest JOB_REQUEST_3 = createTestJobRequest(JOB_REQUEST_3_ID);
    private static final JobRequest JOB_REQUEST_4 = createTestJobRequest(JOB_REQUEST_4_ID);
    private static final JobRequest JOB_REQUEST_5 = createTestJobRequest(JOB_REQUEST_5_ID);

    private static final Set<Cluster> CLUSTERS = Sets.newHashSet(CLUSTER_0, CLUSTER_1, CLUSTER_2);

    private ClusterSelectorScriptProperties scriptProperties;
    private ClusterSelectorManagedScript clusterSelectorScript;
    private PropertiesMapCache cache;

    private static Cluster createTestCluster(final String id) {
        return new Cluster(
            id,
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ClusterMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.UP
            ).build()
        );
    }

    private static JobRequest createTestJobRequest(final String requestedId) {
        return new JobRequest(
            requestedId,
            null,
            null,
            JOB_METADATA,
            CRITERIA,
            null,
            null
        );
    }

    @BeforeEach
    void setUp() {
        final MeterRegistry meterRegistry = new SimpleMeterRegistry();
        final ScriptManagerProperties scriptManagerProperties = new ScriptManagerProperties();
        final TaskScheduler taskScheduler = new ConcurrentTaskScheduler();
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        final ResourceLoader resourceLoader = new DefaultResourceLoader();
        final ScriptManager scriptManager = new ScriptManager(
            scriptManagerProperties,
            taskScheduler,
            executorService,
            scriptEngineManager,
            resourceLoader,
            meterRegistry
        );
        this.scriptProperties = new ClusterSelectorScriptProperties();
        this.cache = Mockito.mock(PropertiesMapCache.class);
        this.clusterSelectorScript = new ClusterSelectorManagedScript(
            scriptManager,
            this.scriptProperties,
            meterRegistry,
            this.cache
        );
    }

    @Test
    void selectClusterTest() throws Exception {
        ManagedScriptIntegrationTest.loadScript(
            "selectCluster.groovy",
            this.clusterSelectorScript,
            this.scriptProperties
        );

        ResourceSelectorScriptResult<Cluster> result;

        result = this.clusterSelectorScript.selectResource(this.createContext(JOB_REQUEST_0_ID));
        Assertions.assertThat(result.getResource()).isPresent().contains(CLUSTER_0);
        Assertions.assertThat(result.getRationale()).isPresent().contains("selected 0");

        result = this.clusterSelectorScript.selectResource(this.createContext(JOB_REQUEST_1_ID));
        Assertions.assertThat(result.getResource()).isNotPresent();
        Assertions.assertThat(result.getRationale()).isPresent().contains("Couldn't find anything");

        Assertions
            .assertThatExceptionOfType(ResourceSelectionException.class)
            .isThrownBy(() -> this.clusterSelectorScript.selectResource(this.createContext(JOB_REQUEST_2_ID)))
            .withNoCause();

        result = this.clusterSelectorScript.selectResource(this.createContext(JOB_REQUEST_3_ID));
        Assertions.assertThat(result.getResource()).isNotPresent();
        Assertions.assertThat(result.getRationale()).isNotPresent();

        Assertions
            .assertThatExceptionOfType(ResourceSelectionException.class)
            .isThrownBy(() -> this.clusterSelectorScript.selectResource(this.createContext(JOB_REQUEST_4_ID)))
            .withCauseInstanceOf(ScriptExecutionException.class);

        // Invalid return type from script
        Assertions
            .assertThatExceptionOfType(ResourceSelectionException.class)
            .isThrownBy(() -> this.clusterSelectorScript.selectResource(this.createContext(JOB_REQUEST_5_ID)));
    }

    private ClusterSelectionContext createContext(final String jobId) {
        final JobRequest jobRequest;
        final boolean apiJob;
        switch (jobId) {
            case JOB_REQUEST_0_ID:
                apiJob = true;
                jobRequest = JOB_REQUEST_0;
                break;
            case JOB_REQUEST_1_ID:
                apiJob = false;
                jobRequest = JOB_REQUEST_1;
                break;
            case JOB_REQUEST_2_ID:
                apiJob = false;
                jobRequest = JOB_REQUEST_2;
                break;
            case JOB_REQUEST_3_ID:
                apiJob = true;
                jobRequest = JOB_REQUEST_3;
                break;
            case JOB_REQUEST_4_ID:
                apiJob = false;
                jobRequest = JOB_REQUEST_4;
                break;
            case JOB_REQUEST_5_ID:
                apiJob = true;
                jobRequest = JOB_REQUEST_5;
                break;
            default:
                throw new IllegalArgumentException(jobId + " is currently unsupported");
        }
        return new ClusterSelectionContext(
            jobId,
            jobRequest,
            apiJob,
            null,
            CLUSTERS
        );
    }
}
