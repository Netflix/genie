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
package com.netflix.genie.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Command;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;


/**
 * Integration Tests for Application Client.
 *
 * @author amsharma
 */
public class ApplicationClientIntegrationTests extends GenieClientsIntegrationTestsBase {

    private CommandClient commandClient;
    private ApplicationClient applicationClient;

    /**
     * Setup for tests.
     *
     * @throws Exception If there is an error.
     */
    @Before
    public void setup() throws Exception {
        commandClient = new CommandClient(getBaseUrl(), null, null);
        applicationClient = new ApplicationClient(getBaseUrl(), null, null);
    }

    /**
     * Delete all applications and applications between tests.
     *
     * @throws Exception If there is any problem.
     */
    @After
    public void cleanUp() throws Exception {
        commandClient.deleteAllCommands();
        applicationClient.deleteAllApplications();
    }

    /**
     * Integration test to get all applications from Genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testCanCreateAndGetApplication() throws Exception {

        final String id = UUID.randomUUID().toString();
        final Application application = constructApplicationDTO(id);

        final String applicationId = applicationClient.createApplication(application);
        Assert.assertEquals(applicationId, id);

        // Make sure Genie Not Found Exception is not thrown for this call.
        final Application cmd = applicationClient.getApplication(id);

        // Make sure the object returned is exactly what was sent to be created
        Assert.assertEquals(application.getId(), cmd.getId());
        Assert.assertEquals(application.getName(), cmd.getName());
        Assert.assertEquals(application.getDescription(), cmd.getDescription());
        Assert.assertEquals(application.getConfigs(), cmd.getConfigs());
        Assert.assertEquals(application.getSetupFile(), cmd.getSetupFile());
        Assert.assertEquals(cmd.getTags().contains("foo"), true);
        Assert.assertEquals(cmd.getTags().contains("bar"), true);
        Assert.assertEquals(application.getStatus(), cmd.getStatus());
    }

    /**
     * Test getting the applications using the various query parameters.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testGetApplicationsUsingParams() throws Exception {
        final String application1Id = UUID.randomUUID().toString();
        final String application2Id = UUID.randomUUID().toString();

        final Set<String> application1Tags = new HashSet<>();
        application1Tags.add("foo");
        application1Tags.add("pi");

        final Set<String> application2Tags = new HashSet<>();
        application2Tags.add("bar");
        application2Tags.add("pi");


        final Application application1 = new Application.Builder(
            "application1name",
            "application1user",
            "1.0",
            ApplicationStatus.ACTIVE
        )
            .withId(application1Id)
            .withTags(application1Tags)
            .build();

        final Application application2 =
            new Application.Builder(
                "application2name",
                "application2user",
                "2.0",
                ApplicationStatus.INACTIVE
            )
                .withId(application2Id)
                .withTags(application2Tags)
                .build();

        applicationClient.createApplication(application1);
        applicationClient.createApplication(application2);

        // Test get by tags
        List<Application> applicationList = applicationClient.getApplications(
            null,
            null,
            null,
            Lists.newArrayList("foo"),
            null
        );
        Assert.assertEquals(1, applicationList.size());
        Assert.assertEquals(application1Id, applicationList.get(0).getId().orElseThrow(IllegalArgumentException::new));

        applicationList = applicationClient.getApplications(
            null,
            null,
            null,
            Lists.newArrayList("pi"),
            null
        );

        Assert.assertEquals(2, applicationList.size());
        Assert.assertEquals(application2Id, applicationList.get(0).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(application1Id, applicationList.get(1).getId().orElseThrow(IllegalArgumentException::new));

        // Test get by name
        applicationList = applicationClient.getApplications(
            "application1name",
            null,
            null,
            null,
            null
        );

        Assert.assertEquals(1, applicationList.size());

        // Test get by status
        applicationList = applicationClient.getApplications(
            null,
            null,
            Lists.newArrayList(ApplicationStatus.ACTIVE.toString()),
            null,
            null
        );

        Assert.assertEquals(1, applicationList.size());

        applicationList = applicationClient.getApplications(
            null,
            null,
            Arrays.asList(ApplicationStatus.ACTIVE.toString(), ApplicationStatus.INACTIVE.toString()),
            null,
            null
        );

        Assert.assertEquals(2, applicationList.size());
    }

    /**
     * Test to confirm getting an exception for non existent application.
     *
     * @throws Exception If there is a problem.
     */
    @Test(expected = IOException.class)
    public void testApplicationNotExist() throws Exception {
        applicationClient.getApplication("foo");
    }

    /**
     * Test get all applications.
     *
     * @throws Exception If there is problem.
     */
    @Test
    public void testGetAllAndDeleteAllApplications() throws Exception {
        final List<Application> initialApplicationList = applicationClient.getApplications();
        Assert.assertEquals(initialApplicationList.size(), 0);

        final Application application1 = constructApplicationDTO(null);
        final Application application2 = constructApplicationDTO(null);

        applicationClient.createApplication(application1);
        applicationClient.createApplication(application2);

        final List<Application> finalApplicationList = applicationClient.getApplications();
        Assert.assertEquals(finalApplicationList.size(), 2);

        Assert.assertEquals(application1.getId(), finalApplicationList.get(1).getId());
        Assert.assertEquals(application2.getId(), finalApplicationList.get(0).getId());

        applicationClient.deleteAllApplications();
        Assert.assertEquals(applicationClient.getApplications().size(), 0);
    }

    /**
     * Test whether we can delete a application in Genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test(expected = IOException.class)
    public void testDeleteApplication() throws Exception {
        final Application application1 = constructApplicationDTO(null);
        applicationClient.createApplication(application1);

        final Application application2
            = applicationClient.getApplication(application1.getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(application2.getId(), application1.getId());

        applicationClient.deleteApplication(application1.getId().orElseThrow(IllegalArgumentException::new));
        applicationClient.getApplication(application1.getId().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test to verify if the update application method is working correctly.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testUpdateApplication() throws Exception {
        final Application application1 = constructApplicationDTO(null);
        applicationClient.createApplication(application1);

        final Application application2
            = applicationClient.getApplication(application1.getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(application2.getName(), application1.getName());

        final Application application3
            = new Application.Builder("newname", "newuser", "new version", ApplicationStatus.ACTIVE)
            .withId(application1.getId().orElseThrow(IllegalArgumentException::new))
            .build();

        applicationClient
            .updateApplication(application1.getId().orElseThrow(IllegalArgumentException::new), application3);

        final Application application4
            = applicationClient.getApplication(application1.getId().orElseThrow(IllegalArgumentException::new));

        Assert.assertEquals("newname", application4.getName());
        Assert.assertEquals("newuser", application4.getUser());
        Assert.assertEquals("new version", application4.getVersion());
        Assert.assertEquals(ApplicationStatus.ACTIVE, application4.getStatus());
        Assert.assertFalse(application4.getSetupFile().isPresent());
        Assert.assertFalse(application4.getDescription().isPresent());
        Assert.assertEquals(Collections.emptySet(), application4.getConfigs());
        Assert.assertEquals(application4.getTags().contains("foo"), false);
    }

    /**
     * Test all the methods that manipulate tags for a application in genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testApplicationTagsMethods() throws Exception {

        final Set<String> initialTags = new HashSet<>();
        initialTags.add("foo");
        initialTags.add("bar");

        final Set<String> configList = new HashSet<>();
        configList.add("config1");
        configList.add("configs2");

        final Application application = new Application.Builder("name", "user", "1.0", ApplicationStatus.ACTIVE)
            .withId("application1")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(initialTags)
            .withConfigs(configList)
            .build();

        applicationClient.createApplication(application);

        // Test getTags for application
        Set<String> tags = applicationClient.getTagsForApplication("application1");
        Assert.assertEquals(4, tags.size());
        Assert.assertEquals(tags.contains("foo"), true);
        Assert.assertEquals(tags.contains("bar"), true);

        // Test adding a tag for application
        final Set<String> moreTags = new HashSet<>();
        moreTags.add("pi");

        applicationClient.addTagsToApplication("application1", moreTags);
        tags = applicationClient.getTagsForApplication("application1");
        Assert.assertEquals(5, tags.size());
        Assert.assertEquals(tags.contains("foo"), true);
        Assert.assertEquals(tags.contains("bar"), true);
        Assert.assertEquals(tags.contains("pi"), true);

        // Test removing a tag for application
        applicationClient.removeTagFromApplication("application1", "bar");
        tags = applicationClient.getTagsForApplication("application1");
        Assert.assertEquals(4, tags.size());
        Assert.assertEquals(tags.contains("foo"), true);
        Assert.assertEquals(tags.contains("pi"), true);

        // Test update tags for a application
        applicationClient.updateTagsForApplication("application1", initialTags);
        tags = applicationClient.getTagsForApplication("application1");
        Assert.assertEquals(4, tags.size());
        Assert.assertEquals(tags.contains("foo"), true);
        Assert.assertEquals(tags.contains("bar"), true);

        // Test delete all tags in a application
        applicationClient.removeAllTagsForApplication("application1");
        tags = applicationClient.getTagsForApplication("application1");
        Assert.assertEquals(2, tags.size());
    }

    /**
     * Test all the methods that manipulate configs for a application in genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testApplicationConfigsMethods() throws Exception {

        final Set<String> initialConfigs = new HashSet<>();
        initialConfigs.add("foo");
        initialConfigs.add("bar");

        final Application application = new Application.Builder("name", "user", "1.0", ApplicationStatus.ACTIVE)
            .withId("application1")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withConfigs(initialConfigs)
            .build();

        applicationClient.createApplication(application);

        // Test getConfigs for application
        Set<String> configs = applicationClient.getConfigsForApplication("application1");
        Assert.assertEquals(2, configs.size());
        Assert.assertEquals(configs.contains("foo"), true);
        Assert.assertEquals(configs.contains("bar"), true);

        // Test adding a config for application
        final Set<String> moreConfigs = new HashSet<>();
        moreConfigs.add("pi");

        applicationClient.addConfigsToApplication("application1", moreConfigs);
        configs = applicationClient.getConfigsForApplication("application1");
        Assert.assertEquals(3, configs.size());
        Assert.assertEquals(configs.contains("foo"), true);
        Assert.assertEquals(configs.contains("bar"), true);
        Assert.assertEquals(configs.contains("pi"), true);

        // Test update configs for a application
        applicationClient.updateConfigsForApplication("application1", initialConfigs);
        configs = applicationClient.getConfigsForApplication("application1");
        Assert.assertEquals(2, configs.size());
        Assert.assertEquals(configs.contains("foo"), true);
        Assert.assertEquals(configs.contains("bar"), true);

        // Test delete all configs in a application
        applicationClient.removeAllConfigsForApplication("application1");
        configs = applicationClient.getConfigsForApplication("application1");
        Assert.assertEquals(0, configs.size());
    }

    /**
     * Test the application patch method.
     *
     * @throws Exception If there is any error.
     */
    @Test
    public void testApplicationPatchMethod() throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        final String newName = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/name\", \"value\": \"" + newName + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(mapper.readTree(patchString));

        final Application application = constructApplicationDTO("application1");

        applicationClient.createApplication(application);
        applicationClient.patchApplication("application1", patch);

        Assert.assertEquals(newName, applicationClient.getApplication("application1").getName());
    }

    /**
     * Test to fetch the commands to which an application is linked.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testCanGetCommandsForApplication() throws Exception {

        final Command command1 = constructCommandDTO(null);
        final Command command2 = constructCommandDTO(null);


        commandClient.createCommand(command1);
        commandClient.createCommand(command2);

        final Application application = constructApplicationDTO(null);

        applicationClient.createApplication(application);

        commandClient.addApplicationsToCommand(
            command1.getId().orElseThrow(IllegalArgumentException::new),
            Lists.newArrayList(application.getId().orElseThrow(IllegalArgumentException::new))
        );
        commandClient.addApplicationsToCommand(
            command2.getId().orElseThrow(IllegalArgumentException::new),
            Lists.newArrayList(application.getId().orElseThrow(IllegalArgumentException::new))
        );

        final List<Command> commandList = applicationClient.getCommandsForApplication(
            application.getId().orElseThrow(IllegalArgumentException::new)
        );

        Assert.assertEquals(2, commandList.size());
    }
}
