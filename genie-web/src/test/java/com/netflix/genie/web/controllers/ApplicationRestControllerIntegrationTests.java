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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.netflix.genie.GenieWeb;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.web.hateoas.resources.ApplicationResource;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.PagedResources;
import org.springframework.hateoas.hal.Jackson2HalModule;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the Applications REST API.
 *
 * @author tgianos
 * @since 3.0.0
 */
//TODO: Add tests for error conditions
@Category(IntegrationTest.class)
@ActiveProfiles({"integration"})
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = GenieWeb.class)
@WebIntegrationTest(randomPort = true)
public class ApplicationRestControllerIntegrationTests {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = "spark";
    private static final String USER = "genie";
    private static final String VERSION = "0.15.0";
    private static final HttpHeaders HEADERS = new HttpHeaders();
    private static final ParameterizedTypeReference<PagedResources<ApplicationResource>> PAGED_TYPE_REFERENCE
            = new ParameterizedTypeReference<PagedResources<ApplicationResource>>() {
    };
    private static RestTemplate restTemplate;

    // Since we're bringing the service up on random port need to figure out what it is
    @Value("${local.server.port}")
    private int port;
    private String appsBaseUrl;
    private String commandsBaseUrl;

    @Autowired
    private JpaApplicationRepository jpaApplicationRepository;

    @Autowired
    private JpaCommandRepository jpaCommandRepository;

    /**
     * Any one time setup for all the tests.
     */
    @BeforeClass
    public static void setupClass() {
        HEADERS.setContentType(MediaType.APPLICATION_JSON);

        // The TestRestTemplate overrides error handler so that errors pass through to user so can validate
        restTemplate = new TestRestTemplate();

        // Add the conversion handler for HAL
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new Jackson2HalModule());

        final MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setSupportedMediaTypes(MediaType.parseMediaTypes(MediaTypes.HAL_JSON_VALUE));
        converter.setObjectMapper(mapper);

        restTemplate.getMessageConverters().add(0, converter);
    }

    /**
     * Setup for tests.
     */
    @Before
    public void setup() {
        this.appsBaseUrl = "http://localhost:" + this.port + "/api/v3/applications";
        this.commandsBaseUrl = "http://localhost:" + this.port + "/api/v3/commands";
    }

    /**
     * Cleanup after tests.
     */
    @After
    public void cleanup() {
        this.jpaCommandRepository.deleteAll();
        this.jpaApplicationRepository.deleteAll();
    }

    /**
     * Test creating an application without an ID.
     *
     * @throws GenieException on configuration issue
     */
    @Test
    public void canCreateApplicationWithoutId() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        final URI location = createApplication(null, NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        final ResponseEntity<ApplicationResource> getResponse
                = restTemplate.getForEntity(location, ApplicationResource.class);

        Assert.assertThat(getResponse.getStatusCode(), Matchers.is(HttpStatus.OK));
        final Application getApp = getResponse.getBody().getContent();
        Assert.assertThat(getApp.getId(), Matchers.notNullValue());
        Assert.assertThat(getApp.getName(), Matchers.is(NAME));
        Assert.assertThat(getApp.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(getApp.getUser(), Matchers.is(USER));
        Assert.assertThat(getApp.getDescription(), Matchers.nullValue());
        Assert.assertThat(getApp.getSetupFile(), Matchers.nullValue());
        Assert.assertThat(getApp.getConfigs().size(), Matchers.is(0));
        Assert.assertThat(getApp.getTags().size(), Matchers.is(2));
        Assert.assertThat(getApp.getTags(), Matchers.hasItem(Matchers.startsWith("genie.id:")));
        Assert.assertThat(getApp.getTags(), Matchers.hasItem(Matchers.startsWith("genie.name:")));
        Assert.assertThat(getApp.getDependencies().size(), Matchers.is(0));
        Assert.assertThat(getApp.getUpdated(), Matchers.notNullValue());
        Assert.assertThat(getApp.getCreated(), Matchers.notNullValue());
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(1L));
    }

    /**
     * Test creating an application with an ID.
     *
     * @throws GenieException When issue in creation
     */
    @Test
    public void canCreateApplicationWithId() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        final URI location = createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        final ResponseEntity<ApplicationResource> getResponse
                = restTemplate.getForEntity(location, ApplicationResource.class);

        Assert.assertThat(getResponse.getStatusCode(), Matchers.is(HttpStatus.OK));
        final Application getApp = getResponse.getBody().getContent();
        Assert.assertThat(getApp.getId(), Matchers.is(ID));
        Assert.assertThat(getApp.getName(), Matchers.is(NAME));
        Assert.assertThat(getApp.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(getApp.getUser(), Matchers.is(USER));
        Assert.assertThat(getApp.getDescription(), Matchers.nullValue());
        Assert.assertThat(getApp.getSetupFile(), Matchers.nullValue());
        Assert.assertThat(getApp.getConfigs().size(), Matchers.is(0));
        Assert.assertThat(getApp.getTags().size(), Matchers.is(2));
        Assert.assertThat(getApp.getTags(), Matchers.hasItem(Matchers.startsWith("genie.id:")));
        Assert.assertThat(getApp.getTags(), Matchers.hasItem(Matchers.startsWith("genie.name:")));
        Assert.assertThat(getApp.getDependencies().size(), Matchers.is(0));
        Assert.assertThat(getApp.getUpdated(), Matchers.notNullValue());
        Assert.assertThat(getApp.getCreated(), Matchers.notNullValue());
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(1L));
    }

    /**
     * Test to make sure the post API can handle bad input.
     */
    @Test
    public void canHandleBadInputToCreateApplication() {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        final Application app = new Application.Builder(null, null, null, null).build();
        final HttpEntity<Application> entity = new HttpEntity<>(app, HEADERS);
        final ResponseEntity<String> responseEntity
                = new TestRestTemplate().postForEntity(this.appsBaseUrl, entity, String.class);

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.PRECONDITION_FAILED));
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can search for applications by various parameters.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canFindApplications() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
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

        createApplication(id1, name1, user1, version1, ApplicationStatus.ACTIVE);
        createApplication(id2, name2, user2, version2, ApplicationStatus.DEPRECATED);
        createApplication(id3, name3, user3, version3, ApplicationStatus.INACTIVE);

        // Test finding all applications
        ResponseEntity<PagedResources<ApplicationResource>> resources = restTemplate.exchange(
                this.appsBaseUrl,
                HttpMethod.GET,
                null,
                PAGED_TYPE_REFERENCE
        );

        Collection<ApplicationResource> apps = resources.getBody().getContent();
        Assert.assertThat(apps.size(), Matchers.is(3));

        // Try to limit the number of results
        URI uri = UriComponentsBuilder.fromHttpUrl(this.appsBaseUrl)
                .queryParam("size", 2)
                .build()
                .encode()
                .toUri();

        resources = restTemplate.exchange(
                uri,
                HttpMethod.GET,
                null,
                PAGED_TYPE_REFERENCE
        );

        apps = resources.getBody().getContent();
        Assert.assertThat(apps.size(), Matchers.is(2));

        // Query by name
        uri = UriComponentsBuilder.fromHttpUrl(this.appsBaseUrl)
                .queryParam("name", name2)
                .build()
                .encode()
                .toUri();
        resources = restTemplate.exchange(
                uri,
                HttpMethod.GET,
                null,
                PAGED_TYPE_REFERENCE
        );
        apps = resources.getBody().getContent();
        Assert.assertThat(apps.size(), Matchers.is(1));
        Assert.assertThat(apps.iterator().next().getContent().getId(), Matchers.is(id2));

        // Query by user
        uri = UriComponentsBuilder.fromHttpUrl(this.appsBaseUrl)
                .queryParam("userName", user3)
                .build()
                .encode()
                .toUri();
        resources = restTemplate.exchange(
                uri,
                HttpMethod.GET,
                null,
                PAGED_TYPE_REFERENCE
        );
        apps = resources.getBody().getContent();
        Assert.assertThat(apps.size(), Matchers.is(1));
        Assert.assertThat(apps.iterator().next().getContent().getId(), Matchers.is(id3));

        // Query by statuses
        uri = UriComponentsBuilder.fromHttpUrl(this.appsBaseUrl)
                .queryParam("status", ApplicationStatus.ACTIVE, ApplicationStatus.DEPRECATED)
                .build()
                .encode()
                .toUri();
        resources = restTemplate.exchange(
                uri,
                HttpMethod.GET,
                null,
                PAGED_TYPE_REFERENCE
        );
        apps = resources.getBody().getContent();
        Assert.assertThat(apps.size(), Matchers.is(2));
        apps.stream()
                .map(ApplicationResource::getContent).
                forEach(
                        application -> {
                            if (!application.getId().equals(id1) && !application.getId().equals(id2)) {
                                Assert.fail();
                            }
                        }
                );

        // Query by tags
        uri = UriComponentsBuilder.fromHttpUrl(this.appsBaseUrl)
                .queryParam("tag", "genie.id:" + id1)
                .build()
                .encode()
                .toUri();
        resources = restTemplate.exchange(
                uri,
                HttpMethod.GET,
                null,
                PAGED_TYPE_REFERENCE
        );
        apps = resources.getBody().getContent();
        Assert.assertThat(apps.size(), Matchers.is(1));
        Assert.assertThat(apps.iterator().next().getContent().getId(), Matchers.is(id1));

        //TODO: Add tests for sort, orderBy etc

        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(3L));
    }

    /**
     * Test to make sure that an application can be updated.
     *
     * @throws GenieException on configuration errorsx
     */
    @Test
    public void canUpdateApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        final URI location = createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        final Application createdApp
                = restTemplate.getForEntity(location, ApplicationResource.class).getBody().getContent();
        Assert.assertThat(createdApp.getStatus(), Matchers.is(ApplicationStatus.ACTIVE));

        final Application newStatusApp = new Application.Builder(
                createdApp.getName(),
                createdApp.getUser(),
                createdApp.getVersion(),
                ApplicationStatus.INACTIVE
        )
                .withId(createdApp.getId())
                .withCreated(createdApp.getCreated())
                .withUpdated(createdApp.getUpdated())
                .withDescription(createdApp.getDescription())
                .withTags(createdApp.getTags())
                .withConfigs(createdApp.getConfigs())
                .withSetupFile(createdApp.getSetupFile())
                .withDependencies(createdApp.getDependencies())
                .build();

        final HttpEntity<Application> entity = new HttpEntity<>(newStatusApp, HEADERS);
        final ResponseEntity<?> updateResponse = restTemplate.exchange(
                this.appsBaseUrl + "/" + ID,
                HttpMethod.PUT,
                entity,
                Void.class
        );

        Assert.assertThat(updateResponse.getStatusCode(), Matchers.is(HttpStatus.NO_CONTENT));
        final Application updateApp
                = restTemplate.getForEntity(this.appsBaseUrl + "/" + ID, Application.class).getBody();
        Assert.assertThat(updateApp.getStatus(), Matchers.is(ApplicationStatus.INACTIVE));
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(1L));
    }

    /**
     * Make sure can successfully delete all applications.
     *
     * @throws GenieException on a configuration error
     */
    @Test
    public void canDeleteAllApplications() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        createApplication(null, NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        createApplication(null, NAME, USER, VERSION, ApplicationStatus.DEPRECATED);
        createApplication(null, NAME, USER, VERSION, ApplicationStatus.INACTIVE);
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(3L));

        restTemplate.delete(this.appsBaseUrl);

        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can delete an application.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canDeleteAnApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
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

        createApplication(id1, name1, user1, version1, ApplicationStatus.ACTIVE);
        createApplication(id2, name2, user2, version2, ApplicationStatus.DEPRECATED);
        createApplication(id3, name3, user3, version3, ApplicationStatus.INACTIVE);
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(3L));

        restTemplate.delete(this.appsBaseUrl + "/" + id2);

        final ResponseEntity<PagedResources<ApplicationResource>> resources = restTemplate.exchange(
                this.appsBaseUrl,
                HttpMethod.GET,
                null,
                PAGED_TYPE_REFERENCE
        );

        final Collection<ApplicationResource> apps = resources.getBody().getContent();
        Assert.assertThat(apps.size(), Matchers.is(2));
        apps.stream()
                .map(ApplicationResource::getContent)
                .forEach(
                        application -> {
                            if (!application.getId().equals(id1) && !application.getId().equals(id3)) {
                                Assert.fail();
                            }
                        }
                );
    }

    /**
     * Test to make sure we can add configurations to the application after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canAddConfigsToApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        ResponseEntity<String[]> configResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/configs",
                String[].class
        );
        Assert.assertThat(configResponse.getBody().length, Matchers.is(0));

        final String config1 = UUID.randomUUID().toString();
        final String config2 = UUID.randomUUID().toString();
        final Set<String> configs = Sets.newHashSet(config1, config2);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(configs, HEADERS);
        restTemplate.postForEntity(
                this.appsBaseUrl + "/" + ID + "/configs",
                entity,
                Void.class
        );

        configResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/configs",
                String[].class
        );

        Assert.assertThat(configResponse.getBody().length, Matchers.is(2));
        Assert.assertTrue(Arrays.asList(configResponse.getBody()).contains(config1));
        Assert.assertTrue(Arrays.asList(configResponse.getBody()).contains(config2));
    }

    /**
     * Test to make sure we can update the configurations for an application after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canUpdateConfigsForApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        final String config1 = UUID.randomUUID().toString();
        final String config2 = UUID.randomUUID().toString();
        HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(config1, config2), HEADERS);
        restTemplate.postForEntity(
                this.appsBaseUrl + "/" + ID + "/configs",
                entity,
                Void.class
        );

        final String config3 = UUID.randomUUID().toString();
        entity = new HttpEntity<>(Sets.newHashSet(config3), HEADERS);
        restTemplate.put(this.appsBaseUrl + "/" + ID + "/configs", entity);

        final ResponseEntity<String[]> configResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/configs",
                String[].class
        );

        Assert.assertThat(configResponse.getBody().length, Matchers.is(1));
        Assert.assertThat(configResponse.getBody()[0], Matchers.is(config3));
    }

    /**
     * Test to make sure we can delete the configurations for an application after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canDeleteConfigsForApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        final String config1 = UUID.randomUUID().toString();
        final String config2 = UUID.randomUUID().toString();
        final HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(config1, config2), HEADERS);
        restTemplate.postForEntity(
                this.appsBaseUrl + "/" + ID + "/configs",
                entity,
                String[].class
        );

        restTemplate.delete(this.appsBaseUrl + "/" + ID + "/configs");

        final ResponseEntity<String[]> configResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/configs",
                String[].class
        );
        Assert.assertThat(configResponse.getBody().length, Matchers.is(0));
    }

    /**
     * Test to make sure we can add dependencies to the application after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canAddDependenciesToApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        ResponseEntity<String[]> dependencyResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/dependencies",
                String[].class
        );
        Assert.assertThat(dependencyResponse.getBody().length, Matchers.is(0));

        final String jar1 = UUID.randomUUID().toString();
        final String jar2 = UUID.randomUUID().toString();
        final Set<String> dependencies = Sets.newHashSet(jar1, jar2);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(dependencies, HEADERS);
        restTemplate.postForEntity(
                this.appsBaseUrl + "/" + ID + "/dependencies",
                entity,
                Void.class
        );

        dependencyResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/dependencies",
                String[].class
        );

        Assert.assertThat(dependencyResponse.getBody().length, Matchers.is(2));
        Assert.assertTrue(Arrays.asList(dependencyResponse.getBody()).contains(jar1));
        Assert.assertTrue(Arrays.asList(dependencyResponse.getBody()).contains(jar2));
    }

    /**
     * Test to make sure we can update the dependencies for an application after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canUpdateDependenciesForApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        final String dependency1 = UUID.randomUUID().toString();
        final String dependency2 = UUID.randomUUID().toString();
        HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(dependency1, dependency2), HEADERS);
        restTemplate.postForEntity(
                this.appsBaseUrl + "/" + ID + "/dependencies",
                entity,
                void.class
        );

        final String dependency3 = UUID.randomUUID().toString();
        entity = new HttpEntity<>(Sets.newHashSet(dependency3), HEADERS);
        restTemplate.put(this.appsBaseUrl + "/" + ID + "/dependencies", entity);
        final ResponseEntity<String[]> dependencyResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/dependencies",
                String[].class
        );

        Assert.assertThat(dependencyResponse.getBody().length, Matchers.is(1));
        Assert.assertThat(dependencyResponse.getBody()[0], Matchers.is(dependency3));
    }

    /**
     * Test to make sure we can delete the dependencies for an application after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canDeleteDependenciesForApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        final String jar1 = UUID.randomUUID().toString();
        final String jar2 = UUID.randomUUID().toString();
        final HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(jar1, jar2), HEADERS);
        restTemplate.postForEntity(
                this.appsBaseUrl + "/" + ID + "/dependencies",
                entity,
                String[].class
        );

        restTemplate.delete(this.appsBaseUrl + "/" + ID + "/dependencies");

        final ResponseEntity<String[]> jarResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/dependencies",
                String[].class
        );
        Assert.assertThat(jarResponse.getBody().length, Matchers.is(0));
    }

    /**
     * Test to make sure we can add tags to the application after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canAddTagsToApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        ResponseEntity<String[]> tagResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/tags",
                String[].class
        );
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(2));

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final Set<String> tags = Sets.newHashSet(tag1, tag2);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(tags, HEADERS);
        restTemplate.postForEntity(
                this.appsBaseUrl + "/" + ID + "/tags",
                entity,
                void.class
        );

        tagResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/tags",
                String[].class
        );

        Assert.assertThat(tagResponse.getBody().length, Matchers.is(4));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag1));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag2));
    }

    /**
     * Test to make sure we can update the tags for an application after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canUpdateTagsForApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(tag1, tag2), HEADERS);
        restTemplate.postForEntity(
                this.appsBaseUrl + "/" + ID + "/tags",
                entity,
                String[].class
        );

        final String tag3 = UUID.randomUUID().toString();
        entity = new HttpEntity<>(Sets.newHashSet(tag3), HEADERS);
        restTemplate.put(this.appsBaseUrl + "/" + ID + "/tags", entity);

        final ResponseEntity<String[]> tagResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/tags",
                String[].class
        );
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(3));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag3));
    }

    /**
     * Test to make sure we can delete the tags for an application after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canDeleteTagsForApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(tag1, tag2), HEADERS);
        restTemplate.postForEntity(
                this.appsBaseUrl + "/" + ID + "/tags",
                entity,
                String[].class
        );

        restTemplate.delete(this.appsBaseUrl + "/" + ID + "/tags");

        final ResponseEntity<String[]> tagResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/tags",
                String[].class
        );
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(2));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
    }

    /**
     * Test to make sure we can delete a tag for an application after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canDeleteTagForApplication() throws GenieException {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(tag1, tag2), HEADERS);
        restTemplate.postForEntity(
                this.appsBaseUrl + "/" + ID + "/tags",
                entity,
                String[].class
        );

        restTemplate.delete(this.appsBaseUrl + "/" + ID + "/tags/" + tag1);

        final ResponseEntity<String[]> tagResponse = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/tags",
                String[].class
        );
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(3));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag2));
    }

    /**
     * Make sure can get all the commands which use a given application.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canGetCommandsForApplication() throws GenieException {
        createApplication(ID, NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        final String placeholder = UUID.randomUUID().toString();
        final String command1Id = UUID.randomUUID().toString();
        final String command2Id = UUID.randomUUID().toString();
        final String command3Id = UUID.randomUUID().toString();
        createCommand(command1Id, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);
        createCommand(command2Id, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);
        createCommand(command3Id, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);

        final Set<String> appIds = Sets.newHashSet(ID);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(appIds, HEADERS);
        restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + command1Id + "/applications",
                entity,
                Application[].class
        );
        restTemplate.postForEntity(
                this.commandsBaseUrl + "/" + command3Id + "/applications",
                entity,
                Application[].class
        );

        final ResponseEntity<Command[]> responseEntity = restTemplate.getForEntity(
                this.appsBaseUrl + "/" + ID + "/commands",
                Command[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(2));
        Arrays.asList(responseEntity.getBody()).stream().forEach(
                command -> {
                    if (!command.getId().equals(command1Id) && !command.getId().equals(command3Id)) {
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
        final HttpEntity<Application> entity = new HttpEntity<>(app, HEADERS);
        return restTemplate.postForLocation(this.appsBaseUrl, entity);
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
        final HttpEntity<Command> entity = new HttpEntity<>(command, HEADERS);
        return restTemplate.postForLocation(this.commandsBaseUrl, entity);
    }
}
