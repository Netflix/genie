/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import retrofit2.Retrofit;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Base class for all Genie client integration tests.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Testcontainers(disabledWithoutDocker = true)
@SuppressWarnings(
    {
        "rawtypes"
    }
)
abstract class GenieClientIntegrationTestBase {

    // Note: Attempt made to make this use the version currently under build however the gradle compiler avoidance
    //       doesn't work right and generates new images every test / build run. This leaves a lot of orphaned
    //       layers on the developers machine which takes up a lot of space. Not currently worth the gain.
    //       Also passing in environment variable from gradle file wasn't being picked up properly in IDE tests for some
    //       reason. For now just leave it hardcoded as the `latest` tags float anyway. This does become problem
    //       for making builds repeatable on tags long term though so it might be better to just periodically update
    //       a static tagged image if we can't get local docker images to reproduce without disk usage overhead.
    // TODO: Improve genie-app image packaging to leverage unpacked (application plugin) based agent so that startup
    //       is faster as in agent mode the tests are much slower than embedded. Also once we move to boot 2.3 we can
    //       leverage their layered jars to produce less changing images.
    @Container
    private static final GenericContainer GENIE = new GenericContainer("netflixoss/genie-app:latest.release")
        .waitingFor(Wait.forHttp("/admin/health").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(1L)))
        .withExposedPorts(8080);

    protected ApplicationClient applicationClient;
    protected CommandClient commandClient;
    protected ClusterClient clusterClient;
    protected JobClient jobClient;

    @BeforeEach
    void setup() throws Exception {
        // Just run these once but don't make it a static BeforeAll in case it would be executed before container starts
        if (
            this.applicationClient == null
                || this.commandClient == null
                || this.clusterClient == null
                || this.jobClient == null
        ) {
            final String baseUrl = "http://" + GENIE.getContainerIpAddress() + ":" + GENIE.getFirstMappedPort();
            final Retrofit retrofit = GenieClientUtils.createRetrofitInstance(baseUrl, null, null);
            if (this.applicationClient == null) {
                this.applicationClient = new ApplicationClient(retrofit);
            }
            if (this.commandClient == null) {
                this.commandClient = new CommandClient(retrofit);
            }
            if (this.clusterClient == null) {
                this.clusterClient = new ClusterClient(retrofit);
            }
            if (this.jobClient == null) {
                this.jobClient = new JobClient(retrofit, 3);
            }
        }
    }

    @AfterEach
    void cleanup() {
        try {
            this.clusterClient.deleteAllClusters();
            this.commandClient.deleteAllCommands();
            this.applicationClient.deleteAllApplications();
        } catch (final Exception e) {
            // swallow
        }
    }

    Cluster constructClusterDTO(final String id) {
        final String clusterId;
        if (StringUtils.isBlank(id)) {
            clusterId = UUID.randomUUID().toString();
        } else {
            clusterId = id;
        }

        final Set<String> tags = Sets.newHashSet("foo", "bar");

        final Set<String> configList = Sets.newHashSet("config1", "configs2");

        final Set<String> dependenciesList = Sets.newHashSet("cluster-dep1", "cluster-dep2");

        return new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withId(clusterId)
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(tags)
            .withConfigs(configList)
            .withDependencies(dependenciesList)
            .build();
    }

    Command constructCommandDTO(final String id) {
        final String commandId;
        if (StringUtils.isBlank(id)) {
            commandId = UUID.randomUUID().toString();
        } else {
            commandId = id;
        }

        final Set<String> tags = Sets.newHashSet("foo", "bar");
        final Set<String> configList = Sets.newHashSet("config1", "configs2");
        final Set<String> dependenciesList = Sets.newHashSet("command-dep1", "command-dep2");
        final List<String> executableAndArgs = Lists.newArrayList("exec");

        return new Command.Builder("name", "user", "1.0", CommandStatus.ACTIVE, executableAndArgs, 1000)
            .withId(commandId)
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(tags)
            .withConfigs(configList)
            .withDependencies(dependenciesList)
            .build();
    }

    Application constructApplicationDTO(final String id) {
        final String applicationId;
        if (StringUtils.isBlank(id)) {
            applicationId = UUID.randomUUID().toString();
        } else {
            applicationId = id;
        }

        final Set<String> tags = Sets.newHashSet("foo", "bar");
        final Set<String> configList = Sets.newHashSet("config1", "configs2");
        final Set<String> dependenciesList = Sets.newHashSet("dep1", "dep2");

        return new Application.Builder("name", "user", "1.0", ApplicationStatus.ACTIVE)
            .withId(applicationId)
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(tags)
            .withConfigs(configList)
            .withDependencies(dependenciesList)
            .build();
    }
}
