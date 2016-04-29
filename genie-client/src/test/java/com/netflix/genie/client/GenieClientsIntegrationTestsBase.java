package com.netflix.genie.client;

import com.netflix.genie.GenieWeb;
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

    protected String getBaseUrl() {
        return "http://localhost:" + port;
    }

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
}
