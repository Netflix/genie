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

import com.google.common.collect.Sets;
import com.netflix.genie.GenieTestApp;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.test.categories.IntegrationTest;
import org.apache.commons.lang3.StringUtils;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Set;
import java.util.UUID;

/**
 * Base class for all Genie client integration tests.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(classes = GenieTestApp.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("integration")
public abstract class GenieClientsIntegrationTestsBase {

    @Value("${local.server.port}")
    private int port;

    /**
     * Helper method that returns the dynamic url of the genie service.
     *
     * @return The genie service url.
     */
    String getBaseUrl() {
        return "http://localhost:" + port;
    }

    /**
     * Helper method to generate a sample Cluster DTO.
     *
     * @param id The id of the cluster.
     * @return A cluster object.
     */
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

    /**
     * Helper method to generate a sample Command DTO.
     *
     * @param id The id of the command.
     * @return A command object.
     */
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

        return new Command.Builder("name", "user", "1.0", CommandStatus.ACTIVE, "exec", 1000)
            .withId(commandId)
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(tags)
            .withConfigs(configList)
            .withDependencies(dependenciesList)
            .build();
    }

    /**
     * Helper method to generate a sample Application DTO.
     *
     * @param id The id of the application.
     * @return An application object.
     */
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
