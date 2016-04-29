package com.netflix.genie.client;

import com.netflix.genie.GenieWeb;
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
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Base class for all Genie client integration tests.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = GenieWeb.class)
@WebIntegrationTest(randomPort = true)
@ActiveProfiles("integration")
@DirtiesContext
public abstract class GenieClientsIntegrationTestsBase {

    @Value("${local.server.port}")
    private int port;

    /**
     * Helper method that returns the dynamic url of the genie service.
     *
     * @return The genie service url.
     */
    protected String getBaseUrl() {
        return "http://localhost:" + port;
    }

    /**
     * Helper method to generate a sample Cluster DTO.
     *
     * @param id The id of the cluster.
     * @return A cluster object.
     */
    protected Cluster constructClusterDTO(final String id) {

        final String clusterId;
        if (StringUtils.isBlank(id)) {
            clusterId = UUID.randomUUID().toString();
        } else {
            clusterId = id;
        }

        final Set<String> tags = new HashSet<>();
        tags.add("foo");
        tags.add("bar");

        final Set<String> configList = new HashSet<>();
        configList.add("config1");
        configList.add("configs2");

        final Cluster cluster = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withId(clusterId)
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(tags)
            .withConfigs(configList)
            .build();

        return cluster;
    }

    /**
     * Helper method to generate a sample Command DTO.
     *
     * @param id The id of the command.
     * @return A command object.
     */
    protected Command constructCommandDTO(final String id) {

        final String commandId;
        if (StringUtils.isBlank(id)) {
            commandId = UUID.randomUUID().toString();
        } else {
            commandId = id;
        }

        final Set<String> tags = new HashSet<>();
        tags.add("foo");
        tags.add("bar");

        final Set<String> configList = new HashSet<>();
        configList.add("config1");
        configList.add("configs2");

        final Command command = new Command.Builder("name", "user", "1.0", CommandStatus.ACTIVE, "exec", 1000)
            .withId(commandId)
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(tags)
            .withConfigs(configList)
            .build();

        return command;
    }

    /**
     * Helper method to generate a sample Application DTO.
     *
     * @param id The id of the application.
     * @return An application object.
     */
    protected Application constructApplicationDTO(final String id) {

        final String applicationId;
        if (StringUtils.isBlank(id)) {
            applicationId = UUID.randomUUID().toString();
        } else {
            applicationId = id;
        }

        final Set<String> tags = new HashSet<>();
        tags.add("foo");
        tags.add("bar");

        final Set<String> configList = new HashSet<>();
        configList.add("config1");
        configList.add("configs2");

        final Set<String> dependenciesList = new HashSet<>();
        dependenciesList.add("dep1");
        dependenciesList.add("dep2");

        final Application application = new Application.Builder("name", "user", "1.0", ApplicationStatus.ACTIVE)
            .withId(applicationId)
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(tags)
            .withConfigs(configList)
            .withDependencies(dependenciesList)
            .build();

        return application;
    }
}
