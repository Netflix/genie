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
package com.netflix.genie.web.controllers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.netflix.genie.GenieWeb;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.test.categories.IntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.Set;
import java.util.UUID;

/**
 * Base class for all integration tests for the controllers.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = GenieWeb.class)
@WebIntegrationTest(randomPort = true)
@ActiveProfiles(resolver = IntegrationTestActiveProfilesResolver.class)
public abstract class RestControllerIntegrationTestsBase {

    protected static final String APPLICATIONS_API = "/api/v3/applications";
    protected static final String CLUSTERS_API = "/api/v3/clusters";
    protected static final String COMMANDS_API = "/api/v3/commands";
    protected static final String JOBS_API = "/api/v3/jobs";

    protected static final String ID_PATH = "$.id";
    protected static final String CREATED_PATH = "$.created";
    protected static final String UPDATED_PATH = "$.updated";
    protected static final String NAME_PATH =  "$.name";
    protected static final String VERSION_PATH =  "$.version";
    protected static final String USER_PATH = "$.user";
    protected static final String DESCRIPTION_PATH = "$.description";
    protected static final String TAGS_PATH = "$.tags";
    protected static final String SETUP_FILE_PATH = "$.setupFile";
    protected static final String STATUS_PATH = "$.status";
    protected static final String CONFIGS_PATH = "$.configs";
    protected static final String LINKS_PATH = "$._links";
    protected static final String EMBEDDED_PATH = "$._embedded";

    // Link Keys
    protected static final String SELF_LINK_KEY = "self";
    protected static final String COMMANDS_LINK_KEY = "commands";
    protected static final String CLUSTERS_LINK_KEY = "clusters";
    protected static final String APPLICATIONS_LINK_KEY = "applications";
    protected static final String JOBS_LINK_KEY = "jobs";

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected MockMvc mvc;

    @Autowired
    private WebApplicationContext context;

    /**
     * Setup class wide configuration.
     */
    @BeforeClass
    public static void setupClass() {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Setup for tests.
     *
     * @throws Exception If there is an error.
     */
    @Before
    public void setup() throws Exception {
        this.mvc = MockMvcBuilders
            .webAppContextSetup(this.context)
            .build();
    }

    protected void canAddElementsToResource(final String api) throws Exception {
        this.mvc
            .perform(MockMvcRequestBuilders.get(api))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));

        final String element1 = UUID.randomUUID().toString();
        final String element2 = UUID.randomUUID().toString();
        final Set<String> elements = Sets.newHashSet(element1, element2);
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(elements))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(api))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(element1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(element2)));
    }

    protected void canUpdateElementsForResource(final String api) throws Exception {
        final String element1 = UUID.randomUUID().toString();
        final String element2 = UUID.randomUUID().toString();
        final Set<String> elements = Sets.newHashSet(element1, element2);
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(elements))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        final String element3 = UUID.randomUUID().toString();
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .put(api)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(Sets.newHashSet(element3)))
            ).andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(api))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(element3)));
    }

    protected void canDeleteElementsFromResource(final String api) throws Exception {
        final String element1 = UUID.randomUUID().toString();
        final String element2 = UUID.randomUUID().toString();
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(Sets.newHashSet(element1, element2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.delete(api))
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(api))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));
    }

    protected void canAddTagsToResource(final String api, final String id, final String name) throws Exception {
        this.mvc
            .perform(MockMvcRequestBuilders.get(api))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.name:" + name)));

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final Set<String> configs = Sets.newHashSet(tag1, tag2);
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(configs))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(api))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(4)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.name:" + name)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(tag1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(tag2)));
    }

    protected void canUpdateTagsForResource(final String api, final String id, final String name) throws Exception {
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final Set<String> tags = Sets.newHashSet(tag1, tag2);
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(tags))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        final String tag3 = UUID.randomUUID().toString();
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .put(api)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(Sets.newHashSet(tag3)))
            ).andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(api))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(3)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.name:" + name)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(tag3)));
    }

    protected void canDeleteTagsForResource(final String api, final String id, final String name) throws Exception {
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(Sets.newHashSet(tag1, tag2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.delete(api))
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(api))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.name:" + name)));
    }

    protected void canDeleteTagForResource(final String api, final String id, final String name) throws Exception {
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(Sets.newHashSet(tag1, tag2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.delete(api + "/" + tag1))
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(MockMvcRequestBuilders.get(api))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(3)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.name:" + name)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(tag2)));
    }

    protected String createApplication(
        final String id,
        final String name,
        final String user,
        final String version,
        final ApplicationStatus status,
        final String type
    ) throws Exception {
        final Application app = new Application.Builder(name, user, version, status).withId(id).withType(type).build();
        final MvcResult result = this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(APPLICATIONS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(app))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();

        return this.getIdFromLocation(result.getResponse().getHeader(HttpHeaders.LOCATION));
    }

    protected String createCluster(
        final String id,
        final String name,
        final String user,
        final String version,
        final ClusterStatus status
    ) throws Exception {
        final Cluster cluster = new Cluster.Builder(name, user, version, status).withId(id).build();
        final MvcResult result = this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(CLUSTERS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(cluster))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();

        return this.getIdFromLocation(result.getResponse().getHeader(HttpHeaders.LOCATION));
    }

    protected String createCommand(
        final String id,
        final String name,
        final String user,
        final String version,
        final CommandStatus status,
        final String executable,
        final long checkDelay
    ) throws Exception {
        final Command command
            = new Command.Builder(name, user, version, status, executable, checkDelay).withId(id).build();
        final MvcResult result = this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(COMMANDS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(command))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();

        return this.getIdFromLocation(result.getResponse().getHeader(HttpHeaders.LOCATION));
    }

    private String getIdFromLocation(final String location) {
        return location.substring(location.lastIndexOf("/") + 1);
    }
}
