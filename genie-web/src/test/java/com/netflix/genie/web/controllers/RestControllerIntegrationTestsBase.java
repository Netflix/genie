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
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.Sets;
import com.netflix.genie.GenieWeb;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.ConfigDTO;
import com.netflix.genie.common.util.GenieDateFormat;
import com.netflix.genie.test.categories.IntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.restdocs.JUnitRestDocumentation;
import org.springframework.restdocs.mockmvc.MockMvcRestDocumentation;
import org.springframework.restdocs.mockmvc.RestDocumentationRequestBuilders;
import org.springframework.restdocs.mockmvc.RestDocumentationResultHandler;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.validation.constraints.NotNull;
import java.util.Set;
import java.util.TimeZone;
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

    static final String APPLICATIONS_API = "/api/v3/applications";
    static final String CLUSTERS_API = "/api/v3/clusters";
    static final String COMMANDS_API = "/api/v3/commands";
    static final String JOBS_API = "/api/v3/jobs";

    static final String ID_PATH = "$.id";
    static final String CREATED_PATH = "$.created";
    static final String UPDATED_PATH = "$.updated";
    static final String NAME_PATH = "$.name";
    static final String VERSION_PATH = "$.version";
    static final String USER_PATH = "$.user";
    static final String DESCRIPTION_PATH = "$.description";
    static final String TAGS_PATH = "$.tags";
    static final String SETUP_FILE_PATH = "$.setupFile";
    static final String STATUS_PATH = "$.status";
    static final String CONFIGS_PATH = "$.configs";
    static final String LINKS_PATH = "$._links";
    static final String EMBEDDED_PATH = "$._embedded";

    // Link Keys
    static final String SELF_LINK_KEY = "self";
    static final String COMMANDS_LINK_KEY = "commands";
    static final String CLUSTERS_LINK_KEY = "clusters";
    static final String APPLICATIONS_LINK_KEY = "applications";
    static final String JOBS_LINK_KEY = "jobs";

    /**
     * Where to put the generated documentation.
     */
    @Rule
    public final JUnitRestDocumentation restDocumentation = new JUnitRestDocumentation("build/generated-snippets");

    protected final ObjectMapper objectMapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setTimeZone(TimeZone.getTimeZone("UTC"))
        .setDateFormat(new GenieDateFormat())
        .registerModule(new Jdk8Module());

    protected MockMvc mvc;

    @Autowired
    private WebApplicationContext context;

    /**
     * Setup for tests.
     *
     * @throws Exception If there is an error.
     */
    @Before
    public void setup() throws Exception {
        this.mvc = MockMvcBuilders
            .webAppContextSetup(this.context)
            .apply(
                MockMvcRestDocumentation
                    .documentationConfiguration(this.restDocumentation)
                    .uris()
                    .withScheme("https")
                    .withHost("genie.example.com")
                    .withPort(443)
            )
            .build();
    }

    void canAddElementsToResource(
        final String api,
        final String id,
        final RestDocumentationResultHandler addHandler,
        final RestDocumentationResultHandler getHandler
    ) throws Exception {
        this.mvc
            .perform(MockMvcRequestBuilders.get(api, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));

        final String element1 = UUID.randomUUID().toString();
        final String element2 = UUID.randomUUID().toString();
        final Set<String> elements = Sets.newHashSet(element1, element2);

        this.mvc
            .perform(
                RestDocumentationRequestBuilders
                    .post(api, id)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(elements))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(addHandler);

        this.mvc
            .perform(RestDocumentationRequestBuilders.get(api, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(element1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(element2)))
            .andDo(getHandler);
    }

    void canUpdateElementsForResource(
        final String api,
        final String id,
        final RestDocumentationResultHandler resultHandler
    ) throws Exception {
        final String element1 = UUID.randomUUID().toString();
        final String element2 = UUID.randomUUID().toString();
        final Set<String> elements = Sets.newHashSet(element1, element2);
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api, id)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(elements))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        final String element3 = UUID.randomUUID().toString();
        this.mvc
            .perform(
                RestDocumentationRequestBuilders
                    .put(api, id)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(Sets.newHashSet(element3)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(resultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(api, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(element3)));
    }

    void canDeleteElementsFromResource(
        final String api,
        final String id,
        final RestDocumentationResultHandler resultHandler
    ) throws Exception {
        final String element1 = UUID.randomUUID().toString();
        final String element2 = UUID.randomUUID().toString();
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api, id)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(Sets.newHashSet(element1, element2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(RestDocumentationRequestBuilders.delete(api, id))
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(resultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(api, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.empty()));
    }

    void canAddTagsToResource(
        final String api,
        final String id,
        final String name,
        final RestDocumentationResultHandler addHandler,
        final RestDocumentationResultHandler getHandler
    ) throws Exception {
        this.mvc
            .perform(MockMvcRequestBuilders.get(api, id))
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
                RestDocumentationRequestBuilders
                    .post(api, id)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(configs))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(addHandler);

        this.mvc
            .perform(RestDocumentationRequestBuilders.get(api, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(4)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.name:" + name)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(tag1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(tag2)))
            .andDo(getHandler);
    }

    void canUpdateTagsForResource(
        final String api,
        final String id,
        final String name,
        final RestDocumentationResultHandler resultHandler
    ) throws Exception {
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final Set<String> tags = Sets.newHashSet(tag1, tag2);
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api, id)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(tags))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        final String tag3 = UUID.randomUUID().toString();
        this.mvc
            .perform(
                RestDocumentationRequestBuilders
                    .put(api, id)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(Sets.newHashSet(tag3)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(resultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(api, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(3)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.name:" + name)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(tag3)));
    }

    void canDeleteTagsForResource(
        final String api,
        final String id,
        final String name,
        final RestDocumentationResultHandler resultHandler
    ) throws Exception {
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api, id)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(Sets.newHashSet(tag1, tag2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(RestDocumentationRequestBuilders.delete(api, id))
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(resultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(api, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.name:" + name)));
    }

    void canDeleteTagForResource(
        final String api,
        final String id,
        final String name,
        final RestDocumentationResultHandler resultHandler
    ) throws Exception {
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(api, id)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(Sets.newHashSet(tag1, tag2)))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        this.mvc
            .perform(RestDocumentationRequestBuilders.delete(api + "/{tag}", id, tag1))
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(resultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(api, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(3)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem("genie.name:" + name)))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasItem(tag2)));
    }

    <R extends ConfigDTO> String createConfigResource(
        @NotNull final R resource,
        final RestDocumentationResultHandler documentationResultHandler
    ) throws Exception {
        final String endpoint;
        if (resource instanceof Application) {
            endpoint = APPLICATIONS_API;
        } else if (resource instanceof Cluster) {
            endpoint = CLUSTERS_API;
        } else if (resource instanceof Command) {
            endpoint = COMMANDS_API;
        } else {
            throw new IllegalArgumentException("Unexpected type: " + resource.getClass().getCanonicalName());
        }

        final ResultActions resultActions = this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(endpoint)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(resource))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()));

        if (documentationResultHandler != null) {
            resultActions.andDo(documentationResultHandler);
        }

        return this.getIdFromLocation(resultActions.andReturn().getResponse().getHeader(HttpHeaders.LOCATION));
    }

    private String getIdFromLocation(final String location) {
        return location.substring(location.lastIndexOf("/") + 1);
    }
}
