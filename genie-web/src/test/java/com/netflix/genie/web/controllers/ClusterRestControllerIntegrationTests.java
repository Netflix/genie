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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.GenieWeb;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import com.netflix.genie.web.hateoas.resources.CommandResource;
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
@Category(IntegrationTest.class)
@ActiveProfiles({"integration"})
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = GenieWeb.class)
@WebIntegrationTest(randomPort = true)
public class ClusterRestControllerIntegrationTests {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = "h2prod";
    private static final String USER = "genie";
    private static final String VERSION = "2.7.1";
    private static final String CLUSTER_TYPE = "yarn";
    private static final HttpHeaders HEADERS = new HttpHeaders();
    private static final ParameterizedTypeReference<PagedResources<ClusterResource>> PAGED_TYPE_REFERENCE
            = new ParameterizedTypeReference<PagedResources<ClusterResource>>() {
    };
    private static RestTemplate restTemplate;

    // Since we're bringing the service up on random port need to figure out what it is
    @Value("${local.server.port}")
    private int port;
    private String commandsBaseUrl;
    private String clustersBaseUrl;

    @Autowired
    private JpaClusterRepository jpaClusterRepository;

    @Autowired
    private JpaCommandRepository jpaCommandRepository;

    /**
     * Setup for all tests.
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
        this.clustersBaseUrl = "http://localhost:" + this.port + "/api/v3/clusters";
        this.commandsBaseUrl = "http://localhost:" + this.port + "/api/v3/commands";
    }

    /**
     * Cleanup after tests.
     */
    @After
    public void cleanup() {
        this.jpaClusterRepository.deleteAll();
        this.jpaCommandRepository.deleteAll();
    }

    /**
     * Test creating a cluster without an ID.
     *
     * @throws GenieException on configuration issue
     */
    @Test
    public void canCreateClusterWithoutId() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        final URI location = createCluster(null, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        final Cluster cluster = restTemplate.getForEntity(location, ClusterResource.class).getBody().getContent();
        Assert.assertThat(cluster.getId(), Matchers.is(Matchers.notNullValue()));
        Assert.assertThat(cluster.getName(), Matchers.is(NAME));
        Assert.assertThat(cluster.getUser(), Matchers.is(USER));
        Assert.assertThat(cluster.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(cluster.getStatus(), Matchers.is(ClusterStatus.UP));
        Assert.assertThat(cluster.getClusterType(), Matchers.is(CLUSTER_TYPE));
        Assert.assertThat(cluster.getTags(), Matchers.hasItem(Matchers.startsWith("genie.id:")));
        Assert.assertThat(cluster.getTags(), Matchers.hasItem(Matchers.startsWith("genie.name:")));
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(1L));
    }

    /**
     * Test creating a Cluster with an ID.
     *
     * @throws GenieException When issue in creation
     */
    @Test
    public void canCreateClusterWithId() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        final URI location = createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        final Cluster cluster = restTemplate.getForEntity(location, ClusterResource.class).getBody().getContent();
        Assert.assertThat(cluster.getId(), Matchers.is(ID));
        Assert.assertThat(cluster.getName(), Matchers.is(NAME));
        Assert.assertThat(cluster.getUser(), Matchers.is(USER));
        Assert.assertThat(cluster.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(cluster.getStatus(), Matchers.is(ClusterStatus.UP));
        Assert.assertThat(cluster.getClusterType(), Matchers.is(CLUSTER_TYPE));
        Assert.assertThat(cluster.getTags().size(), Matchers.is(2));
        Assert.assertThat(cluster.getTags(), Matchers.hasItem(Matchers.startsWith("genie.id:")));
        Assert.assertThat(cluster.getTags(), Matchers.hasItem(Matchers.startsWith("genie.name:")));
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(1L));
    }

    /**
     * Test to make sure the post API can handle bad input.
     */
    @Test
    public void canHandleBadInputToCreateCluster() {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        final HttpEntity<Cluster> entity = new HttpEntity<>(
                new Cluster.Builder(null, null, null, null, null).build(),
                HEADERS
        );
        final ResponseEntity<String> responseEntity
                = new TestRestTemplate().postForEntity(this.clustersBaseUrl, entity, String.class);

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.PRECONDITION_FAILED));
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can search for clusters by various parameters.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canFindClusters() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
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
        final String clusterType1 = UUID.randomUUID().toString();
        final String clusterType2 = UUID.randomUUID().toString();
        final String clusterType3 = UUID.randomUUID().toString();

        createCluster(id1, name1, user1, version1, ClusterStatus.UP, clusterType1);
        createCluster(id2, name2, user2, version2, ClusterStatus.OUT_OF_SERVICE, clusterType2);
        createCluster(id3, name3, user3, version3, ClusterStatus.TERMINATED, clusterType3);

        // Test finding all clusters
        ResponseEntity<PagedResources<ClusterResource>> resources = restTemplate.exchange(
                this.clustersBaseUrl,
                HttpMethod.GET,
                null,
                PAGED_TYPE_REFERENCE
        );

        Assert.assertThat(resources.getStatusCode(), Matchers.is(HttpStatus.OK));
        Collection<ClusterResource> clusters = resources.getBody().getContent();
        Assert.assertThat(clusters.size(), Matchers.is(3));

        // Try to limit the number of results
        URI uri = UriComponentsBuilder.fromHttpUrl(this.clustersBaseUrl)
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

        Assert.assertThat(resources.getStatusCode(), Matchers.is(HttpStatus.OK));
        clusters = resources.getBody().getContent();
        Assert.assertThat(clusters.size(), Matchers.is(2));

        // Query by name
        uri = UriComponentsBuilder.fromHttpUrl(this.clustersBaseUrl)
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
        clusters = resources.getBody().getContent();
        Assert.assertThat(clusters.size(), Matchers.is(1));
        Assert.assertThat(clusters.iterator().next().getContent().getId(), Matchers.is(id2));

        // Query by statuses
        uri = UriComponentsBuilder.fromHttpUrl(this.clustersBaseUrl)
                .queryParam("status", ClusterStatus.UP, ClusterStatus.TERMINATED)
                .build()
                .encode()
                .toUri();
        resources = restTemplate.exchange(
                uri,
                HttpMethod.GET,
                null,
                PAGED_TYPE_REFERENCE
        );
        clusters = resources.getBody().getContent();
        Assert.assertThat(clusters.size(), Matchers.is(2));
        clusters.stream().forEach(
                cluster -> {
                    if (!cluster.getContent().getId().equals(id1) && !cluster.getContent().getId().equals(id3)) {
                        Assert.fail();
                    }
                }
        );

        // Query by tags
        uri = UriComponentsBuilder.fromHttpUrl(this.clustersBaseUrl)
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
        clusters = resources.getBody().getContent();
        Assert.assertThat(clusters.size(), Matchers.is(1));
        Assert.assertThat(clusters.iterator().next().getContent().getId(), Matchers.is(id1));

        //TODO: Add tests for searching by min and max update time as those are available parameters
        //TODO: Add tests for sort, orderBy etc

        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(3L));
    }

    /**
     * Test to make sure that a cluster can be updated.
     *
     * @throws GenieException on configuration errors
     */
    @Test
    public void canUpdateCluster() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        final URI location = createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        final Cluster createdCluster
                = restTemplate.getForEntity(location, ClusterResource.class).getBody().getContent();
        Assert.assertThat(createdCluster.getStatus(), Matchers.is(ClusterStatus.UP));

        final Cluster updateCluster = new Cluster.Builder(
                createdCluster.getName(),
                createdCluster.getUser(),
                createdCluster.getVersion(),
                ClusterStatus.OUT_OF_SERVICE,
                createdCluster.getClusterType()
        )
                .withId(createdCluster.getId())
                .withCreated(createdCluster.getCreated())
                .withUpdated(createdCluster.getUpdated())
                .withDescription(createdCluster.getDescription())
                .withTags(createdCluster.getTags())
                .withConfigs(createdCluster.getConfigs())
                .build();

        final HttpEntity<Cluster> entity = new HttpEntity<>(updateCluster, HEADERS);
        restTemplate.put(location, entity);

        final ResponseEntity<Cluster> updateResponse = restTemplate.getForEntity(location, Cluster.class);
        Assert.assertThat(updateResponse.getStatusCode(), Matchers.is(HttpStatus.OK));
        final Cluster updatedCluster = updateResponse.getBody();
        Assert.assertThat(updatedCluster.getStatus(), Matchers.is(ClusterStatus.OUT_OF_SERVICE));
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(1L));
    }

    /**
     * Make sure can successfully delete all clusters.
     *
     * @throws GenieException on a configuration error
     */
    @Test
    public void canDeleteAllClusters() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        createCluster(null, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);
        createCluster(null, NAME, USER, VERSION, ClusterStatus.OUT_OF_SERVICE, CLUSTER_TYPE);
        createCluster(null, NAME, USER, VERSION, ClusterStatus.TERMINATED, CLUSTER_TYPE);
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(3L));

        restTemplate.delete(this.clustersBaseUrl);

        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can delete a cluster.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canDeleteACluster() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
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
        final String clusterType1 = UUID.randomUUID().toString();
        final String clusterType2 = UUID.randomUUID().toString();
        final String clusterType3 = UUID.randomUUID().toString();

        createCluster(id1, name1, user1, version1, ClusterStatus.UP, clusterType1);
        createCluster(id2, name2, user2, version2, ClusterStatus.OUT_OF_SERVICE, clusterType2);
        createCluster(id3, name3, user3, version3, ClusterStatus.TERMINATED, clusterType3);
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(3L));

        restTemplate.delete(this.clustersBaseUrl + "/" + id2);

        final ResponseEntity<PagedResources<ClusterResource>> resources = restTemplate.exchange(
                this.clustersBaseUrl,
                HttpMethod.GET,
                null,
                PAGED_TYPE_REFERENCE
        );

        Assert.assertThat(resources.getStatusCode(), Matchers.is(HttpStatus.OK));
        final Collection<ClusterResource> clusters = resources.getBody().getContent();
        Assert.assertThat(clusters.size(), Matchers.is(2));
        clusters.stream().forEach(
                cluster -> {
                    if (!cluster.getContent().getId().equals(id1) && !cluster.getContent().getId().equals(id3)) {
                        Assert.fail();
                    }
                }
        );
    }

    /**
     * Test to make sure we can add configurations to the cluster after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canAddConfigsToCluster() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        ResponseEntity<String[]> configResponse = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/configs",
                String[].class
        );
        Assert.assertThat(configResponse.getBody().length, Matchers.is(0));

        final String config1 = UUID.randomUUID().toString();
        final String config2 = UUID.randomUUID().toString();
        final Set<String> configs = Sets.newHashSet(config1, config2);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(configs, HEADERS);
        restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + ID + "/configs",
                entity,
                void.class
        );
        configResponse = restTemplate.getForEntity(this.clustersBaseUrl + "/" + ID + "/configs", String[].class);

        Assert.assertThat(configResponse.getBody().length, Matchers.is(2));
        Assert.assertTrue(Arrays.asList(configResponse.getBody()).contains(config1));
        Assert.assertTrue(Arrays.asList(configResponse.getBody()).contains(config2));
    }

    /**
     * Test to make sure we can update the configurations for a cluster after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canUpdateConfigsForCluster() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        final String config1 = UUID.randomUUID().toString();
        final String config2 = UUID.randomUUID().toString();
        HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(config1, config2), HEADERS);
        restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + ID + "/configs",
                entity,
                void.class
        );

        final String config3 = UUID.randomUUID().toString();
        entity = new HttpEntity<>(Sets.newHashSet(config3), HEADERS);
        restTemplate.put(
                this.clustersBaseUrl + "/" + ID + "/configs",
                entity
        );

        final ResponseEntity<String[]> configResponse
                = restTemplate.getForEntity(this.clustersBaseUrl + "/" + ID + "/configs", String[].class);

        Assert.assertThat(configResponse.getBody().length, Matchers.is(1));
        Assert.assertThat(configResponse.getBody()[0], Matchers.is(config3));
    }

    /**
     * Test to make sure we can delete the configurations for a cluster after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canDeleteConfigsForCluster() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        final String config1 = UUID.randomUUID().toString();
        final String config2 = UUID.randomUUID().toString();
        final HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(config1, config2), HEADERS);
        restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + ID + "/configs",
                entity,
                String[].class
        );

        restTemplate.delete(this.clustersBaseUrl + "/" + ID + "/configs");

        final ResponseEntity<String[]> configResponse = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/configs",
                String[].class
        );
        Assert.assertThat(configResponse.getBody().length, Matchers.is(0));
    }

    /**
     * Test to make sure we can add tags to the cluster after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canAddTagsToCluster() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        ResponseEntity<String[]> tagResponse = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/tags",
                String[].class
        );
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(2));

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final Set<String> tags = Sets.newHashSet(tag1, tag2);
        final HttpEntity<Set<String>> entity = new HttpEntity<>(tags, HEADERS);
        restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + ID + "/tags",
                entity,
                void.class
        );

        tagResponse = restTemplate.getForEntity(this.clustersBaseUrl + "/" + ID + "/tags", String[].class);
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(4));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag1));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag2));
    }

    /**
     * Test to make sure we can update the tags for a cluster after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canUpdateTagsForCluster() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(tag1, tag2), HEADERS);
        restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + ID + "/tags",
                entity,
                String[].class
        );

        final String tag3 = UUID.randomUUID().toString();
        entity = new HttpEntity<>(Sets.newHashSet(tag3), HEADERS);
        restTemplate.put(
                this.clustersBaseUrl + "/" + ID + "/tags",
                entity
        );

        final ResponseEntity<String[]> tagResponse
                = restTemplate.getForEntity(this.clustersBaseUrl + "/" + ID + "/tags", String[].class);

        Assert.assertThat(tagResponse.getBody().length, Matchers.is(3));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag3));
    }

    /**
     * Test to make sure we can delete the tags for a cluster after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canDeleteTagsForCluster() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(tag1, tag2), HEADERS);
        restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + ID + "/tags",
                entity,
                String[].class
        );

        restTemplate.delete(this.clustersBaseUrl + "/" + ID + "/tags");

        final ResponseEntity<String[]> tagResponse = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/tags",
                String[].class
        );
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(2));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
    }

    /**
     * Test to make sure we can delete a tag for a cluster after it is created.
     *
     * @throws GenieException on configuration problems
     */
    @Test
    public void canDeleteTagForCluster() throws GenieException {
        Assert.assertThat(this.jpaClusterRepository.count(), Matchers.is(0L));
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final HttpEntity<Set<String>> entity = new HttpEntity<>(Sets.newHashSet(tag1, tag2), HEADERS);
        restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + ID + "/tags",
                entity,
                String[].class
        );

        restTemplate.delete(this.clustersBaseUrl + "/" + ID + "/tags/" + tag1);

        final ResponseEntity<String[]> tagResponse = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/tags",
                String[].class
        );
        Assert.assertThat(tagResponse.getBody().length, Matchers.is(3));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.id:" + ID));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains("genie.name:" + NAME));
        Assert.assertTrue(Arrays.asList(tagResponse.getBody()).contains(tag2));
    }

    /**
     * Make sure can add the commands for a cluster.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canAddCommandsForACluster() throws GenieException {
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);
        final ResponseEntity<Command[]> emptyCommandResponse = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                Command[].class
        );
        Assert.assertThat(emptyCommandResponse.getBody().length, Matchers.is(0));

        final String placeholder = UUID.randomUUID().toString();
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        createCommand(commandId1, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);
        createCommand(commandId2, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);

        final List<String> commandIds = Lists.newArrayList(commandId1, commandId2);
        final HttpEntity<List<String>> entity = new HttpEntity<>(commandIds, HEADERS);
        restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                entity,
                Command[].class
        );

        ResponseEntity<CommandResource[]> responseEntity = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                CommandResource[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(2));
        Assert.assertThat(responseEntity.getBody()[0].getContent().getId(), Matchers.is(commandId1));
        Assert.assertThat(responseEntity.getBody()[1].getContent().getId(), Matchers.is(commandId2));

        //Shouldn't add anything
        commandIds.clear();
        final ResponseEntity<String> errorResponse = new TestRestTemplate().postForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                entity,
                String.class
        );
        Assert.assertThat(errorResponse.getStatusCode(), Matchers.is(HttpStatus.PRECONDITION_FAILED));
        responseEntity = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                CommandResource[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(2));

        final String commandId3 = UUID.randomUUID().toString();
        createCommand(commandId3, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);
        commandIds.add(commandId3);
        restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                entity,
                Command[].class
        );

        responseEntity = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                CommandResource[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(3));
        Assert.assertThat(responseEntity.getBody()[0].getContent().getId(), Matchers.is(commandId1));
        Assert.assertThat(responseEntity.getBody()[1].getContent().getId(), Matchers.is(commandId2));
        Assert.assertThat(responseEntity.getBody()[2].getContent().getId(), Matchers.is(commandId3));
    }

    /**
     * Make sure can handle bad input to add commands.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canHandleBadInputToAddCommandsForACluster() throws GenieException {
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);
        final HttpEntity<List<String>> entity = new HttpEntity<>(null, HEADERS);
        final ResponseEntity<String> responseEntity = new TestRestTemplate().postForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                entity,
                String.class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.BAD_REQUEST));
    }

    /**
     * Make sure can set the commands for a cluster.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canSetCommandsForACluster() throws GenieException {
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);
        final ResponseEntity<Command[]> emptyAppResponse = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                Command[].class
        );
        Assert.assertThat(emptyAppResponse.getBody().length, Matchers.is(0));

        final String placeholder = UUID.randomUUID().toString();
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        createCommand(commandId1, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);
        createCommand(commandId2, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);

        final List<String> commandIds = Lists.newArrayList(commandId1, commandId2);
        final HttpEntity<List<String>> entity = new HttpEntity<>(commandIds, HEADERS);
        restTemplate.exchange(
                this.clustersBaseUrl + "/" + ID + "/commands",
                HttpMethod.PUT,
                entity,
                Command[].class
        );

        ResponseEntity<CommandResource[]> responseEntity = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                CommandResource[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(2));
        Assert.assertThat(responseEntity.getBody()[0].getContent().getId(), Matchers.is(commandId1));
        Assert.assertThat(responseEntity.getBody()[1].getContent().getId(), Matchers.is(commandId2));

        //Should clear apps
        commandIds.clear();
        restTemplate.exchange(
                this.clustersBaseUrl + "/" + ID + "/commands",
                HttpMethod.PUT,
                entity,
                Command[].class
        );
        responseEntity = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                CommandResource[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(0));

        final String commandId3 = UUID.randomUUID().toString();
        createCommand(commandId3, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);
        commandIds.add(commandId3);
        restTemplate.exchange(
                this.clustersBaseUrl + "/" + ID + "/commands",
                HttpMethod.PUT,
                entity,
                Command[].class
        );

        responseEntity = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                CommandResource[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(1));
        Assert.assertThat(responseEntity.getBody()[0].getContent().getId(), Matchers.is(commandId3));
    }

    /**
     * Make sure that we can remove all the commands from a cluster.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canRemoveCommandsFromACluster() throws GenieException {
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        final String placeholder = UUID.randomUUID().toString();
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        createCommand(commandId1, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);
        createCommand(commandId2, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);

        final List<String> commandIds = Lists.newArrayList(commandId1, commandId2);
        final HttpEntity<List<String>> entity = new HttpEntity<>(commandIds, HEADERS);
        restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                entity,
                Command[].class
        );

        restTemplate.delete(this.clustersBaseUrl + "/" + ID + "/commands");
        final ResponseEntity<CommandResource[]> responseEntity = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                CommandResource[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(0));
    }

    /**
     * Make sure that we can remove a command from a cluster.
     *
     * @throws GenieException on configuration error
     */
    @Test
    public void canRemoveCommandFromACluster() throws GenieException {
        createCluster(ID, NAME, USER, VERSION, ClusterStatus.UP, CLUSTER_TYPE);

        final String placeholder = UUID.randomUUID().toString();
        final String commandId1 = UUID.randomUUID().toString();
        final String commandId2 = UUID.randomUUID().toString();
        final String commandId3 = UUID.randomUUID().toString();
        createCommand(commandId1, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);
        createCommand(commandId2, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);
        createCommand(commandId3, placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder);

        final List<String> commandIds = Lists.newArrayList(commandId1, commandId2, commandId3);
        final HttpEntity<List<String>> entity = new HttpEntity<>(commandIds, HEADERS);
        restTemplate.postForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                entity,
                Command[].class
        );

        restTemplate.delete(this.clustersBaseUrl + "/" + ID + "/commands/" + commandId2);
        final ResponseEntity<CommandResource[]> responseEntity = restTemplate.getForEntity(
                this.clustersBaseUrl + "/" + ID + "/commands",
                CommandResource[].class
        );

        Assert.assertThat(responseEntity.getStatusCode(), Matchers.is(HttpStatus.OK));
        Assert.assertThat(responseEntity.getBody().length, Matchers.is(2));
        Assert.assertThat(responseEntity.getBody()[0].getContent().getId(), Matchers.is(commandId1));
        Assert.assertThat(responseEntity.getBody()[1].getContent().getId(), Matchers.is(commandId3));
        Assert.assertThat(this.jpaCommandRepository.count(), Matchers.is(3L));
        final ResponseEntity<Cluster[]> command1Clusters = restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + commandId1 + "/clusters",
                Cluster[].class
        );
        Assert.assertThat(command1Clusters.getBody().length, Matchers.is(1));
        Assert.assertThat(command1Clusters.getBody()[0].getId(), Matchers.is(ID));
        final ResponseEntity<Cluster[]> command2Clusters = restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + commandId2 + "/clusters",
                Cluster[].class
        );
        Assert.assertThat(command2Clusters.getBody().length, Matchers.is(0));
        final ResponseEntity<Cluster[]> command3Clusters = restTemplate.getForEntity(
                this.commandsBaseUrl + "/" + commandId3 + "/clusters",
                Cluster[].class
        );
        Assert.assertThat(command3Clusters.getBody().length, Matchers.is(1));
        Assert.assertThat(command3Clusters.getBody()[0].getId(), Matchers.is(ID));
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
        final HttpEntity<Cluster> entity = new HttpEntity<>(cluster, HEADERS);
        return restTemplate.postForLocation(this.clustersBaseUrl, entity);
    }
}
