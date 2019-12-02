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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.ClusterMetadata;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.properties.ClusterLoadBalancerScriptProperties;
import com.netflix.genie.web.properties.ScriptManagerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

class ClusterLoadBalancerScriptIntegrationTest {

    private static final Cluster CLUSTER_0 = new Cluster(
        "0",
        Instant.now(),
        Instant.now(),
        new ExecutionEnvironment(null, null, null),
        new ClusterMetadata.Builder(
            "d",
            "e",
            "f",
            ClusterStatus.UP
        ).build()
    );
    private static final Cluster CLUSTER_1 = new Cluster(
        "1",
        Instant.now(),
        Instant.now(),
        new ExecutionEnvironment(null, null, null),
        new ClusterMetadata.Builder(
            "g",
            "h",
            "i",
            ClusterStatus.UP
        ).build()
    );
    private static final Cluster CLUSTER_2 = new Cluster(
        "2",
        Instant.now(),
        Instant.now(),
        new ExecutionEnvironment(null, null, null),
        new ClusterMetadata.Builder(
            "a",
            "b",
            "c",
            ClusterStatus.UP
        ).build()
    );


    private static final JobRequest JOB_REQUEST = new JobRequest.Builder(
        "jobName",
        "jobUser",
        "jobVersion",
        Lists.newArrayList(
            new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
        ),
        Sets.newHashSet(UUID.randomUUID().toString())
    ).build();

    private static final Set<Cluster> ALL_CLUSTERS = Sets.newHashSet(
        CLUSTER_0, CLUSTER_1, CLUSTER_2
    );

    private static final Set<Cluster> NO_MATCH_CLUSTERS = Sets.newHashSet(
        CLUSTER_0, CLUSTER_2
    );

    private ClusterLoadBalancerScriptProperties scriptProperties;
    private ClusterLoadBalancerScript clusterLoadBalancerScript;

    @BeforeEach
    void setUp() {
        final MeterRegistry meterRegistry = new SimpleMeterRegistry();
        final ScriptManagerProperties scriptManagerProperties = new ScriptManagerProperties();
        final TaskScheduler taskScheduler =  new ConcurrentTaskScheduler();
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        final ResourceLoader resourceLoader = new DefaultResourceLoader();
        final ObjectMapper objectMapper = GenieObjectMapper.getMapper();
        final ScriptManager scriptManager = new ScriptManager(
            scriptManagerProperties,
            taskScheduler,
            executorService,
            scriptEngineManager,
            resourceLoader,
            meterRegistry
        );
        this.scriptProperties = new ClusterLoadBalancerScriptProperties();
        this.clusterLoadBalancerScript = new ClusterLoadBalancerScript(
            scriptManager,
            scriptProperties,
            objectMapper,
            meterRegistry
        );
    }

    @ParameterizedTest(name = "Select using {0}")
    @ValueSource(strings = {"loadBalance.js", "loadBalance.groovy"})
    void selectClusterTest(
        final String scriptFilename
    ) throws Exception {
        ManagedScriptIntegrationTest.loadScript(scriptFilename,  clusterLoadBalancerScript, scriptProperties);

        Assertions.assertThat(
            this.clusterLoadBalancerScript.selectCluster(JOB_REQUEST, ALL_CLUSTERS)
        ).isEqualTo(CLUSTER_1);

        Assertions.assertThat(
            this.clusterLoadBalancerScript.selectCluster(JOB_REQUEST, NO_MATCH_CLUSTERS)
        ).isEqualTo(null);
    }
}
