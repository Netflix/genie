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
package com.netflix.genie.web.controllers;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jpa.repositories.ApplicationRepository;
import com.netflix.genie.core.jpa.repositories.ClusterRepository;
import com.netflix.genie.core.jpa.repositories.CommandRepository;
import com.netflix.genie.web.configs.GenieConfig;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the Commands REST API.
 *
 * @author tgianos
 * @since 3.0.0
 */
//TODO: Add tests for error conditions
@ActiveProfiles({"integration"})
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = GenieConfig.class)
@WebIntegrationTest(randomPort = true)
public class CommandControllerIntegrationTests {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = "hive";
    private static final String USER = "genie";
    private static final String VERSION = "1.0.0";
    private static final String EXECUTABLE = "/apps/hive/bin/hive";

    // The TestRestTemplate overrides error handler so that errors pass through to user so can validate
    private final RestTemplate restTemplate = new TestRestTemplate();
    private final HttpHeaders headers = new HttpHeaders();

    // Since we're bringing the service up on random port need to figure out what it is
    @Value("${local.server.port}")
    private int port;
    private String appsBaseUrl;
    private String commandsBaseUrl;
    private String clustersBaseUrl;

    @Autowired
    private ApplicationRepository applicationRepository;

    @Autowired
    private ClusterRepository clusterRepository;

    @Autowired
    private CommandRepository commandRepository;

    /**
     * Setup for tests.
     */
    @Before
    public void setup() {
        this.appsBaseUrl = "http://localhost:" + this.port + "/api/v3/applications";
        this.clustersBaseUrl = "http://localhost:" + this.port + "/api/v3/clusters";
        this.commandsBaseUrl = "http://localhost:" + this.port + "/api/v3/commands";
        this.headers.setContentType(MediaType.APPLICATION_JSON);
    }

    /**
     * Cleanup after tests.
     */
    @After
    public void cleanup() {
        this.clusterRepository.deleteAll();
        this.commandRepository.deleteAll();
        this.applicationRepository.deleteAll();
    }

    /**
     * Test creating a command without an ID.
     *
     * @throws GenieException on configuration issue
     */
    @Test
    public void canCreateCommandWithoutId() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        final URI location = createCommand(null, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        final Command command = this.restTemplate.getForEntity(location, Command.class).getBody();
        Assert.assertThat(command.getId(), Matchers.is(Matchers.notNullValue()));
        Assert.assertThat(command.getName(), Matchers.is(NAME));
        Assert.assertThat(command.getUser(), Matchers.is(USER));
        Assert.assertThat(command.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(command.getStatus(), Matchers.is(CommandStatus.ACTIVE));
        Assert.assertThat(command.getExecutable(), Matchers.is(EXECUTABLE));
        Assert.assertThat(command.getTags(), Matchers.hasItem(Matchers.startsWith("genie.id:")));
        Assert.assertThat(command.getTags(), Matchers.hasItem(Matchers.startsWith("genie.name:")));
        Assert.assertThat(this.commandRepository.count(), Matchers.is(1L));
    }

    /**
     * Test creating a Command with an ID.
     *
     * @throws GenieException When issue in creation
     */
    @Test
    public void canCreateCommandWithId() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        final URI location = createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        final Command command = this.restTemplate.getForEntity(location, Command.class).getBody();
        Assert.assertThat(command.getId(), Matchers.is(ID));
        Assert.assertThat(command.getName(), Matchers.is(NAME));
        Assert.assertThat(command.getUser(), Matchers.is(USER));
        Assert.assertThat(command.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(command.getStatus(), Matchers.is(CommandStatus.ACTIVE));
        Assert.assertThat(command.getExecutable(), Matchers.is(EXECUTABLE));
        Assert.assertThat(command.getTags().size(), Matchers.is(2));
        Assert.assertThat(command.getTags(), Matchers.hasItem(Matchers.startsWith("genie.id:")));
        Assert.assertThat(command.getTags(), Matchers.hasItem(Matchers.startsWith("genie.name:")));
        Assert.assertThat(this.commandRepository.count(), Matchers.is(1L));
    }

    /**
     * Test to make sure the post API can handle bad input.
     */
    @Test
    public void canHandleBadInputToCreateCommand() {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        final Command command = new Command.Builder(null, null, null, null, null).build();
        final HttpEntity<Command> entity = new HttpEntity<>(command, this.headers);
        final ResponseEntity<String> responseEntity
                = this.restTemplate.postForEntity(this.commandsBaseUrl, entity, String.class);

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.PRECONDITION_FAILED));
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can search for commands by various parameters.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canFindCommands() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        final String id1 = UUID.randomUUID().toString();
        final String id2 = UUID.randomUUID().toString();
        final String id3 = UUID.randomUUID().toString();
        final String name1 = UUID.randomUUID().toString();
        final String name2 = UUID.randomUUID().toString();
        final String name3 = UUID.randomUUID().toString();
        final String user1 = UUID.randomUUID().toString();
        final String user2 = UUID.randomUUID().toString();
        final String user3 = UUID.randomUUID().toString();
        final String version1 = UUID.randomUUID().toString();
        final String version2 = UUID.randomUUID().toString();
        final String version3 = UUID.randomUUID().toString();
        final String executable1 = UUID.randomUUID().toString();
        final String executable2 = UUID.randomUUID().toString();
        final String executable3 = UUID.randomUUID().toString();

        createCommand(id1, name1, user1, version1, CommandStatus.ACTIVE, executable1);
        createCommand(id2, name2, user2, version2, CommandStatus.DEPRECATED, executable2);
        createCommand(id3, name3, user3, version3, CommandStatus.INACTIVE, executable3);

        // Test finding all commands
        ResponseEntity<Command[]> getResponse
                = this.restTemplate.getForEntity(this.commandsBaseUrl, Command[].class);

        Assert.assertThat(getResponse.getStatusCode(), Matchers.is(HttpStatus.OK));
        Command[] commands = getResponse.getBody();
        Assert.assertThat(commands.length, Matchers.is(3));

        // Try to limit the number of results
        URI uri = UriComponentsBuilder.fromHttpUrl(this.commandsBaseUrl)
                .queryParam("limit", 2)
                .build()
                .encode()
                .toUri();
        getResponse = this.restTemplate.getForEntity(uri, Command[].class);

        Assert.assertThat(getResponse.getStatusCode(), Matchers.is(HttpStatus.OK));
        commands = getResponse.getBody();
        Assert.assertThat(commands.length, Matchers.is(2));

        // Query by name
        uri = UriComponentsBuilder.fromHttpUrl(this.commandsBaseUrl)
                .queryParam("name", name2)
                .build()
                .encode()
                .toUri();
        getResponse = this.restTemplate.getForEntity(uri, Command[].class);
        commands = getResponse.getBody();
        Assert.assertThat(commands.length, Matchers.is(1));
        Assert.assertThat(commands[0].getId(), Matchers.is(id2));

        // Query by user
        uri = UriComponentsBuilder.fromHttpUrl(this.commandsBaseUrl)
                .queryParam("userName", user3)
                .build()
                .encode()
                .toUri();
        getResponse = this.restTemplate.getForEntity(uri, Command[].class);
        commands = getResponse.getBody();
        Assert.assertThat(commands.length, Matchers.is(1));
        Assert.assertThat(commands[0].getId(), Matchers.is(id3));

        // Query by statuses
        uri = UriComponentsBuilder.fromHttpUrl(this.commandsBaseUrl)
                .queryParam("status", CommandStatus.ACTIVE, CommandStatus.INACTIVE)
                .build()
                .encode()
                .toUri();
        getResponse = this.restTemplate.getForEntity(uri, Command[].class);
        commands = getResponse.getBody();
        Assert.assertThat(commands.length, Matchers.is(2));
        Arrays.asList(commands).stream().forEach(
                command -> {
                    if (!command.getId().equals(id1) && !command.getId().equals(id3)) {
                        Assert.fail();
                    }
                }
        );

        // Query by tags
        uri = UriComponentsBuilder.fromHttpUrl(this.commandsBaseUrl)
                .queryParam("tag", "genie.id:" + id1)
                .build()
                .encode()
                .toUri();
        getResponse = this.restTemplate.getForEntity(uri, Command[].class);
        commands = getResponse.getBody();
        Assert.assertThat(commands.length, Matchers.is(1));
        Assert.assertThat(commands[0].getId(), Matchers.is(id1));

        //TODO: Add tests for sort, orderBy etc

        Assert.assertThat(this.commandRepository.count(), Matchers.is(3L));
    }

    /**
     * Test to make sure that a command can be updated.
     *
     * @throws GenieException on configuration errors
     */
    @Test
    public void canUpdateCommand() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        final URI location = createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        final Command createdCommand = this.restTemplate.getForEntity(location, Command.class).getBody();
        Assert.assertThat(createdCommand.getStatus(), Matchers.is(CommandStatus.ACTIVE));

        final Command updateCommand = new Command.Builder(
                createdCommand.getName(),
                createdCommand.getUser(),
                createdCommand.getVersion(),
                CommandStatus.INACTIVE,
                createdCommand.getExecutable()
        )
                .withId(createdCommand.getId())
                .withCreated(createdCommand.getCreated())
                .withUpdated(createdCommand.getUpdated())
                .withDescription(createdCommand.getDescription())
                .withTags(createdCommand.getTags())
                .withConfigs(createdCommand.getConfigs())
                .withSetupFile(createdCommand.getSetupFile())
                .withJobType(createdCommand.getJobType())
                .build();
        final HttpEntity<Command> entity = new HttpEntity<>(updateCommand, this.headers);
        this.restTemplate.put(location, entity);

        final Command updatedCommand = this.restTemplate.getForEntity(location, Command.class).getBody();
        Assert.assertThat(updatedCommand.getStatus(), Matchers.is(CommandStatus.INACTIVE));
        Assert.assertThat(this.commandRepository.count(), Matchers.is(1L));
    }

    /**
     * Make sure can successfully delete all commands.
     *
     * @throws GenieException on a configuration error
     */
    @Test
    public void canDeleteAllCommands() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        createCommand(null, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);
        createCommand(null, NAME, USER, VERSION, CommandStatus.DEPRECATED, EXECUTABLE);
        createCommand(null, NAME, USER, VERSION, CommandStatus.INACTIVE, EXECUTABLE);
        Assert.assertThat(this.commandRepository.count(), Matchers.is(3L));

        this.restTemplate.delete(this.commandsBaseUrl);

        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can delete a command.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canDeleteACommand() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        final String id1 = UUID.randomUUID().toString();
        final String id2 = UUID.randomUUID().toString();
        final String id3 = UUID.randomUUID().toString();
        final String name1 = UUID.randomUUID().toString();
        final String name2 = UUID.randomUUID().toString();
        final String name3 = UUID.randomUUID().toString();
        final String user1 = UUID.randomUUID().toString();
        final String user2 = UUID.randomUUID().toString();
        final String user3 = UUID.randomUUID().toString();
        final String version1 = UUID.randomUUID().toString();
        final String version2 = UUID.randomUUID().toString();
        final String version3 = UUID.randomUUID().toString();
        final String executable1 = UUID.randomUUID().toString();
        final String executable2 = UUID.randomUUID().toString();
        final String executable3 = UUID.randomUUID().toString();

        createCommand(id1, name1, user1, version1, CommandStatus.ACTIVE, executable1);
        createCommand(id2, name2, user2, version2, CommandStatus.DEPRECATED, executable2);
        createCommand(id3, name3, user3, version3, CommandStatus.INACTIVE, executable3);
        Assert.assertThat(this.commandRepository.count(), Matchers.is(3L));

        this.restTemplate.delete(this.commandsBaseUrl + "/" + id2);

        final ResponseEntity<Command[]> getResponse
                = this.restTemplate.getForEntity(this.commandsBaseUrl, Command[].class);

        Assert.assertThat(getResponse.getStatusCode(), Matchers.is(HttpStatus.OK));
        final Command[] commands = getResponse.getBody();
        Assert.assertThat(commands.length, Matchers.is(2));
        Arrays.asList(commands).stream().forEach(
                command -> {
                    if (!command.getId().equals(id1) && !command.getId().equals(id3)) {
                        Assert.fail();
                    }
                }
        );
    }

    /**
     * Test to make sure we can add configurations to the command after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canAddConfigsToCommand() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        final ResponseEntity<String[]> configResponse = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/configs",
                String[].class
        );
        Assert.assertThat(configResponse.getBody().length, Matchers.is(0));

        final String config1 = UUID.randomUUID().toString();
        final String config2 = UUID.randomUUID().toString();
        final Set<String> configs = Sets.newHashSet(config1, config2);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(configs, this.headers);
        this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/configs",
                entity,
                void.class
        );

        final List<String> finalConfigs = Arrays.asList(
                this.restTemplate.getForEntity(
                        this.commandsBaseUrl + "/" + ID + "/configs",
                        String[].class
                ).getBody()
        );
        Assert.assertThat(finalConfigs.size(), Matchers.is(2));
        Assert.assertTrue(finalConfigs.contains(config1));
        Assert.assertTrue(finalConfigs.contains(config2));
    }

    /**
     * Test to make sure we can update the configurations for a command after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canUpdateConfigsForCommand() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        final String config1 = UUID.randomUUID().toString();
        final String config2 = UUID.randomUUID().toString();
        HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(config1, config2), this.headers);
        this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/configs",
                entity,
                String[].class
        );

        final String config3 = UUID.randomUUID().toString();
        entity = new HttpEntity<>(Sets.newHashSet(config3), this.headers);
        this.restTemplate.put(
                this.commandsBaseUrl + "/" + ID + "/configs",
                entity
        );

        final ResponseEntity<String[]> configResponse
                = this.restTemplate.getForEntity(this.commandsBaseUrl + "/" + ID + "/configs", String[].class);

        Assert.assertThat(configResponse.getBody().length, Matchers.is(1));
        Assert.assertThat(configResponse.getBody()[0], Matchers.is(config3));
    }

    /**
     * Test to make sure we can delete the configurations for a command after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canDeleteConfigsForCommand() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        final String config1 = UUID.randomUUID().toString();
        final String config2 = UUID.randomUUID().toString();
        final HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(config1, config2), this.headers);
        this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/configs",
                entity,
                void.class
        );

        this.restTemplate.delete(this.commandsBaseUrl + "/" + ID + "/configs");

        final ResponseEntity<String[]> configResponse = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/configs",
                String[].class
        );
        Assert.assertThat(configResponse.getBody().length, Matchers.is(0));
    }

    /**
     * Test to make sure we can add tags to the command after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canAddTagsToCommand() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        ResponseEntity<String[]> tagResponse = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/tags",
                String[].class
        );
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(2));

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final Set<String> tags = Sets.newHashSet(tag1, tag2);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(tags, this.headers);
        this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/tags",
                entity,
                void.class
        );

        tagResponse = this.restTemplate.getForEntity(this.commandsBaseUrl + "/" + ID + "/tags", String[].class);

        Assert.assertThat(tagResponse.getBody().length, Matchers.is(4));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag1));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag2));
    }

    /**
     * Test to make sure we can update the tags for a command after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canUpdateTagsForCommand() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(tag1, tag2), this.headers);
        this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/tags",
                entity,
                void.class
        );

        final String tag3 = UUID.randomUUID().toString();
        entity = new HttpEntity<>(Sets.newHashSet(tag3), this.headers);
        this.restTemplate.put(this.commandsBaseUrl + "/" + ID + "/tags", entity);

        final ResponseEntity<String[]> tagResponse
                = this.restTemplate.getForEntity(this.commandsBaseUrl + "/" + ID + "/tags", String[].class);
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(3));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag3));
    }

    /**
     * Test to make sure we can delete the tags for a command after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canDeleteTagsForCommand() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(tag1, tag2), this.headers);
        this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/tags",
                entity,
                String[].class
        );

        this.restTemplate.delete(this.commandsBaseUrl + "/" + ID + "/tags");

        final ResponseEntity<String[]> tagResponse = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/tags",
                String[].class
        );
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(2));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
    }

    /**
     * Test to make sure we can delete a tag for a command after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canDeleteTagForCommand() throws GenieException {
        Assert.assertThat(this.commandRepository.count(), Matchers.is(0L));
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(tag1, tag2), this.headers);
        this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/tags",
                entity,
                String[].class
        );

        this.restTemplate.delete(this.commandsBaseUrl + "/" + ID + "/tags/" + tag1);

        final ResponseEntity<String[]> tagResponse = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/tags",
                String[].class
        );
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(3));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag2));
    }

    /**
     * Make sure can add the applications for a command.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canAddApplicationsForACommand() throws GenieException {
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);
        final ResponseEntity<Application[]> emptyAppResponse = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                Application[].class
        );
        Assert.assertThat(emptyAppResponse.getBody().length, Matchers.is(0));

        final String placeholder = UUID.randomUUID().toString();
        final String applicationId1 = UUID.randomUUID().toString();
        final String applicationId2 = UUID.randomUUID().toString();
        createApplication(applicationId1, placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE);
        createApplication(applicationId2, placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE);

        final Set<String> appIds = Sets.newHashSet(applicationId1, applicationId2);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(appIds, this.headers);
        this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                entity,
                Application[].class
        );

        ResponseEntity<Application[]> responseEntity = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                Application[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(2));
        Arrays.asList(responseEntity.getBody()).stream().forEach(
                application -> {
                    if (!application.getId().equals(applicationId1) && !application.getId().equals(applicationId2)) {
                        Assert.fail();
                    }
                }
        );

        //Shouldn't add anything
        appIds.clear();
        final ResponseEntity<String> errorResponse = this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                entity,
                String.class
        );
        Assert.assertThat(errorResponse.getStatusCode(), Matchers.is(HttpStatus.PRECONDITION_FAILED));
        responseEntity = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                Application[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(2));

        final String applicationId3 = UUID.randomUUID().toString();
        createApplication(applicationId3, placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE);
        appIds.add(applicationId3);
        this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                entity,
                Application[].class
        );

        responseEntity = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                Application[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(3));
    }

    /**
     * Make sure can handle bad input to add applications.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canHandleBadInputToAddApplicationsForACommand() throws GenieException {
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(null, this.headers);
        final ResponseEntity<String> responseEntity = this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                entity,
                String.class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.BAD_REQUEST));
    }

    /**
     * Make sure can set the applications for a command.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canSetApplicationsForACommand() throws GenieException {
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);
        final ResponseEntity<Application[]> emptyAppResponse = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                Application[].class
        );
        Assert.assertThat(emptyAppResponse.getBody().length, Matchers.is(0));

        final String placeholder = UUID.randomUUID().toString();
        final String applicationId1 = UUID.randomUUID().toString();
        final String applicationId2 = UUID.randomUUID().toString();
        createApplication(applicationId1, placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE);
        createApplication(applicationId2, placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE);

        final Set<String> appIds = Sets.newHashSet(applicationId1, applicationId2);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(appIds, this.headers);
        this.restTemplate.exchange(
                this.commandsBaseUrl + "/" + ID + "/applications",
                HttpMethod.PUT,
                entity,
                Application[].class
        );

        ResponseEntity<Application[]> responseEntity = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                Application[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(2));
        Arrays.asList(responseEntity.getBody()).stream().forEach(
                application -> {
                    if (!application.getId().equals(applicationId1) && !application.getId().equals(applicationId2)) {
                        Assert.fail();
                    }
                }
        );

        //Should clear apps
        appIds.clear();
        this.restTemplate.exchange(
                this.commandsBaseUrl + "/" + ID + "/applications",
                HttpMethod.PUT,
                entity,
                Application[].class
        );
        responseEntity = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                Application[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(0));

        final String applicationId3 = UUID.randomUUID().toString();
        createApplication(applicationId3, placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE);
        appIds.add(applicationId3);
        this.restTemplate.exchange(
                this.commandsBaseUrl + "/" + ID + "/applications",
                HttpMethod.PUT,
                entity,
                Application[].class
        );

        responseEntity = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                Application[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(1));
        Assert.assertThat(responseEntity.getBody()[0].getId(), Matchers.is(applicationId3));
    }

    /**
     * Make sure that we can remove all the applications from a command.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canRemoveApplicationsFromACommand() throws GenieException {
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        final String placeholder = UUID.randomUUID().toString();
        final String applicationId1 = UUID.randomUUID().toString();
        final String applicationId2 = UUID.randomUUID().toString();
        createApplication(applicationId1, placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE);
        createApplication(applicationId2, placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE);

        final Set<String> appIds = Sets.newHashSet(applicationId1, applicationId2);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(appIds, this.headers);
        this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                entity,
                Application[].class
        );

        this.restTemplate.delete(this.commandsBaseUrl + "/" + ID + "/applications");
        final ResponseEntity<Application[]> responseEntity = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                Application[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(0));
    }

    /**
     * Make sure that we can remove an application from a command.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canRemoveApplicationFromACommand() throws GenieException {
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);

        final String placeholder = UUID.randomUUID().toString();
        final String applicationId1 = UUID.randomUUID().toString();
        final String applicationId2 = UUID.randomUUID().toString();
        createApplication(applicationId1, placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE);
        createApplication(applicationId2, placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE);

        final Set<String> appIds = Sets.newHashSet(applicationId1, applicationId2);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(appIds, this.headers);
        this.restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                entity,
                Application[].class
        );

        this.restTemplate.delete(this.commandsBaseUrl + "/" + ID + "/applications/" + applicationId1);
        final ResponseEntity<Application[]> responseEntity = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/applications",
                Application[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(1));
        Assert.assertThat(responseEntity.getBody()[0].getId(), Matchers.is(applicationId2));
        Assert.assertThat(this.applicationRepository.count(), Matchers.is(2L));
        final ResponseEntity<Command[]> application1Commands = this.restTemplate.getForEntity(
                this.appsBaseUrl + "/" + applicationId1 + "/commands",
                Command[].class
        );
        Assert.assertThat(application1Commands.getBody().length, Matchers.is(0));
        final ResponseEntity<Command[]> application2Commands = this.restTemplate.getForEntity(
                this.appsBaseUrl + "/" + applicationId2 + "/commands",
                Command[].class
        );
        Assert.assertThat(application2Commands.getBody().length, Matchers.is(1));
    }

    /**
     * Make sure can get all the clusters which use a given command.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canGetClustersForCommand() throws GenieException {
        createCommand(ID, NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);
        final String placeholder = UUID.randomUUID().toString();
        final String cluster1Id = UUID.randomUUID().toString();
        final String cluster2Id = UUID.randomUUID().toString();
        final String cluster3Id = UUID.randomUUID().toString();
        createCluster(cluster1Id, placeholder, placeholder, placeholder, ClusterStatus.UP, placeholder);
        createCluster(cluster2Id, placeholder, placeholder, placeholder, ClusterStatus.UP, placeholder);
        createCluster(cluster3Id, placeholder, placeholder, placeholder, ClusterStatus.UP, placeholder);

        final List<String> commandIds = Lists.newArrayList(ID);
        final HttpEntity<List<String>> entity = new HttpEntity<>(commandIds, this.headers);
        this.restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + cluster1Id + "/commands",
                entity,
                Command[].class
        );
        this.restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + cluster3Id + "/commands",
                entity,
                Command[].class
        );

        final ResponseEntity<Cluster[]> responseEntity = this.restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + ID + "/clusters",
                Cluster[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(2));
        Arrays.asList(responseEntity.getBody()).stream().forEach(
                cluster -> {
                    if (!cluster.getId().equals(cluster1Id) && !cluster.getId().equals(cluster3Id)) {
                        Assert.fail();
                    }
                }
        );
    }

    /**
     * Helper for creating an application used in testing.
     *
     * @param id      The id to use for the application or null/empty/blank for one to be assigned
     * @param name    The name to use for the application
     * @param user    The user to use for the application
     * @param version The version to use for the application
     * @param status  The status to use for the application
     * @throws GenieException for any misconfiguration
     */
    private URI createApplication(
            final String id,
            final String name,
            final String user,
            final String version,
            final ApplicationStatus status
    ) throws GenieException {
        final Application app = new Application.Builder(name, user, version, status).withId(id).build();
        final HttpEntity<Application> entity = new HttpEntity<>(app, this.headers);
        return this.restTemplate.postForLocation(this.appsBaseUrl, entity);
    }

    /**
     * Helper for creating a command used in testing.
     *
     * @param id         The id to use for the command or null/empty/blank for one to be assigned
     * @param name       The name to use for the command
     * @param user       The user to use for the command
     * @param version    The version to use for the command
     * @param status     The status to use for the command
     * @param executable The executable to use for the command
     * @throws GenieException for any misconfiguration
     */
    private URI createCommand(
            final String id,
            final String name,
            final String user,
            final String version,
            final CommandStatus status,
            final String executable
    ) throws GenieException {
        final Command command = new Command.Builder(name, user, version, status, executable).withId(id).build();
        final HttpEntity<Command> entity = new HttpEntity<>(command, this.headers);
        return this.restTemplate.postForLocation(this.commandsBaseUrl, entity);
    }

    /**
     * Helper for creating a cluster used in testing.
     *
     * @param id          The id to use for the cluster or null/empty/blank for one to be assigned
     * @param name        The name to use for the cluster
     * @param user        The user to use for the cluster
     * @param version     The version to use for the cluster
     * @param status      The status to use for the cluster
     * @param clusterType The type of the cluster e.g. yarn or presto
     * @throws GenieException for any misconfiguration
     */
    private URI createCluster(
            final String id,
            final String name,
            final String user,
            final String version,
            final ClusterStatus status,
            final String clusterType
    ) throws GenieException {
        final Cluster cluster = new Cluster.Builder(name, user, version, status, clusterType).withId(id).build();
        final HttpEntity<Cluster> entity = new HttpEntity<>(cluster, this.headers);
        return this.restTemplate.postForLocation(this.clustersBaseUrl, entity);
    }
}
