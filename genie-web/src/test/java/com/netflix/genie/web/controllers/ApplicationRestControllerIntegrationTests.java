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

import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.web.hateoas.resources.ApplicationResource;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.MediaType;
import org.springframework.restdocs.mockmvc.MockMvcRestDocumentation;
import org.springframework.restdocs.mockmvc.RestDocumentationRequestBuilders;
import org.springframework.restdocs.mockmvc.RestDocumentationResultHandler;
import org.springframework.restdocs.operation.preprocess.Preprocessors;
import org.springframework.restdocs.payload.PayloadDocumentation;
import org.springframework.restdocs.request.RequestDocumentation;
import org.springframework.restdocs.snippet.Attributes;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the Applications REST API.
 *
 * @author tgianos
 * @since 3.0.0
 */
//TODO: Add tests for error conditions
public class ApplicationRestControllerIntegrationTests extends RestControllerIntegrationTestsBase {

    private static final String ID = UUID.randomUUID().toString();
    private static final String NAME = "spark";
    private static final String USER = "genie";
    private static final String VERSION = "1.5.1";
    private static final String TYPE = "spark";

    private static final String TYPE_PATH = "$.type";

    private static final String APPLICATIONS_LIST_PATH = EMBEDDED_PATH + ".applicationList";
    private static final String APPLICATIONS_ID_LIST_PATH = EMBEDDED_PATH + ".applicationList..id";
    private static final String APPLICATION_COMMANDS_LINK_PATH = "$._links.commands.href";
    private static final String APPLICATIONS_COMMANDS_LINK_PATH = "$.._links.commands.href";

    @Autowired
    private JpaApplicationRepository jpaApplicationRepository;

    @Autowired
    private JpaCommandRepository jpaCommandRepository;

    @Autowired
    private JpaFileRepository fileRepository;

    @Autowired
    private JpaTagRepository tagRepository;

    /**
     * Common setup for all tests.
     */
    @Before
    public void setup() {
        this.jpaCommandRepository.deleteAll();
        this.jpaApplicationRepository.deleteAll();
        this.fileRepository.deleteAll();
        this.tagRepository.deleteAll();
    }

    /**
     * Cleanup after tests.
     */
    @After
    public void cleanup() {
        this.jpaCommandRepository.deleteAll();
        this.jpaApplicationRepository.deleteAll();
        this.fileRepository.deleteAll();
        this.tagRepository.deleteAll();
    }

    /**
     * Test creating an application without an ID.
     *
     * @throws Exception on configuration issue
     */
    @Test
    public void canCreateApplicationWithoutId() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));

        final RestDocumentationResultHandler creationResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // Request headers
            Snippets.getApplicationRequestPayload(), // Request fields
            Snippets.LOCATION_HEADER // Response headers
        );

        final String id = this.createConfigResource(
            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withType(TYPE)
                .withDependencies(Sets.newHashSet("s3://mybucket/spark/" + VERSION + "/spark.tar.gz"))
                .withSetupFile("s3://mybucket/spark/" + VERSION + "/setup-spark.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/spark/" + VERSION + "/spark-env.sh"))
                .withDescription("Spark for Genie")
                .withTags(Sets.newHashSet("type:" + TYPE, "ver:" + VERSION))
                .build(),
            creationResultHandler
        );

        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // response headers
            Snippets.getApplicationResponsePayload(), // response payload
            Snippets.APPLICATION_LINKS // response links
        );

        this.mvc
            .perform(RestDocumentationRequestBuilders.get(APPLICATIONS_API + "/{id}", id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(id)))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(VERSION_PATH, Matchers.is(VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(USER)))
            .andExpect(MockMvcResultMatchers.jsonPath(DESCRIPTION_PATH, Matchers.is("Spark for Genie")))
            .andExpect(MockMvcResultMatchers
                .jsonPath(
                    SETUP_FILE_PATH,
                    Matchers.is("s3://mybucket/spark/" + VERSION + "/setup-spark.sh")
                )
            )
            .andExpect(MockMvcResultMatchers
                .jsonPath(
                    CONFIGS_PATH,
                    Matchers.hasItem("s3://mybucket/spark/" + VERSION + "/spark-env.sh")
                )
            )
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasSize(4)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.id:" + id)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("ver:" + VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("type:" + TYPE)))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(ApplicationStatus.ACTIVE.toString())))
            .andExpect(MockMvcResultMatchers
                .jsonPath(
                    DEPENDENCIES_PATH,
                    Matchers.hasItem("s3://mybucket/spark/" + VERSION + "/spark.tar.gz")
                )
            )
            .andExpect(MockMvcResultMatchers.jsonPath(TYPE_PATH, Matchers.is(TYPE)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH + ".*", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATION_COMMANDS_LINK_PATH,
                EntityLinkMatcher.matchUri(
                    APPLICATIONS_API, COMMANDS_LINK_KEY, COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS, id)))
            .andDo(getResultHandler);

        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(1L));
    }

    /**
     * Test creating an application with an ID.
     *
     * @throws Exception When issue in creation
     */
    @Test
    public void canCreateApplicationWithId() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));

        this.createConfigResource(
            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withId(ID)
                .withType(TYPE)
                .withDependencies(Sets.newHashSet("s3://mybucket/spark/" + VERSION + "/spark.tar.gz"))
                .withSetupFile("s3://mybucket/spark/" + VERSION + "/setup-spark.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/spark/" + VERSION + "/spark-env.sh"))
                .withDescription("Spark for Genie")
                .withTags(Sets.newHashSet("type:" + TYPE, "ver:" + VERSION))
                .build(),
            null
        );

        this.mvc
            .perform(MockMvcRequestBuilders.get(APPLICATIONS_API + "/" + ID))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(ID)))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(VERSION_PATH, Matchers.is(VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(USER)))
            .andExpect(MockMvcResultMatchers.jsonPath(DESCRIPTION_PATH, Matchers.is("Spark for Genie")))
            .andExpect(MockMvcResultMatchers
                .jsonPath(
                    SETUP_FILE_PATH,
                    Matchers.is("s3://mybucket/spark/" + VERSION + "/setup-spark.sh")
                )
            )
            .andExpect(MockMvcResultMatchers
                .jsonPath(
                    CONFIGS_PATH,
                    Matchers.hasItem("s3://mybucket/spark/" + VERSION + "/spark-env.sh")
                )
            )
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasSize(4)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.id:" + ID)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("genie.name:" + NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("ver:" + VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath(TAGS_PATH, Matchers.hasItem("type:" + TYPE)))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(ApplicationStatus.ACTIVE.toString())))
            .andExpect(MockMvcResultMatchers
                .jsonPath(
                    DEPENDENCIES_PATH,
                    Matchers.hasItem("s3://mybucket/spark/" + VERSION + "/spark.tar.gz")
                )
            )
            .andExpect(MockMvcResultMatchers.jsonPath(TYPE_PATH, Matchers.is(TYPE)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH + ".*", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATION_COMMANDS_LINK_PATH,
                EntityLinkMatcher.matchUri(
                    APPLICATIONS_API, COMMANDS_LINK_KEY, COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS, ID)));

        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(1L));
    }

    /**
     * Test to make sure the post API can handle bad input.
     *
     * @throws Exception on issue
     */
    @Test
    public void canHandleBadInputToCreateApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        final Application app = new Application.Builder(null, null, null, null).build();
        this.mvc.perform(
            MockMvcRequestBuilders
                .post(APPLICATIONS_API)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsBytes(app))
        ).andExpect(MockMvcResultMatchers.status().isPreconditionFailed());
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can search for applications by various parameters.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canFindApplications() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        final Application spark151 = new Application.Builder("spark", "genieUser1", "1.5.1", ApplicationStatus.ACTIVE)
            .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.5.1.tar.gz"))
            .withSetupFile("s3://mybucket/spark/setup-spark.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
            .withDescription("Spark 1.5.1 for Genie")
            .withTags(Sets.newHashSet("type:spark", "ver:1.5.1"))
            .withType("spark")
            .build();

        final Application spark150 = new Application.Builder("spark", "genieUser2", "1.5.0", ApplicationStatus.ACTIVE)
            .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.5.0.tar.gz"))
            .withSetupFile("s3://mybucket/spark/setup-spark.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
            .withDescription("Spark 1.5.0 for Genie")
            .withTags(Sets.newHashSet("type:spark", "ver:1.5.0"))
            .withType("spark")
            .build();

        final Application spark141 = new Application.Builder("spark", "genieUser3", "1.4.1", ApplicationStatus.INACTIVE)
            .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.4.1.tar.gz"))
            .withSetupFile("s3://mybucket/spark/setup-spark.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
            .withDescription("Spark 1.4.1 for Genie")
            .withTags(Sets.newHashSet("type:spark", "ver:1.4.1"))
            .withType("spark")
            .build();

        final Application spark140
            = new Application.Builder("spark", "genieUser4", "1.4.0", ApplicationStatus.DEPRECATED)
            .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.4.0.tar.gz"))
            .withSetupFile("s3://mybucket/spark/setup-spark.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
            .withDescription("Spark 1.4.0 for Genie")
            .withTags(Sets.newHashSet("type:spark", "ver:1.4.0"))
            .withType("spark")
            .build();

        final Application spark131
            = new Application.Builder("spark", "genieUser5", "1.3.1", ApplicationStatus.DEPRECATED)
            .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.3.1.tar.gz"))
            .withSetupFile("s3://mybucket/spark/setup-spark.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
            .withDescription("Spark 1.3.1 for Genie")
            .withTags(Sets.newHashSet("type:spark", "ver:1.3.1"))
            .withType("spark")
            .build();

        final Application pig = new Application.Builder("spark", "genieUser6", "0.4.0", ApplicationStatus.ACTIVE)
            .withDependencies(Sets.newHashSet("s3://mybucket/pig/pig-0.15.0.tar.gz"))
            .withSetupFile("s3://mybucket/pig/setup-pig.sh")
            .withConfigs(Sets.newHashSet("s3://mybucket/pig/pig.properties"))
            .withDescription("Pig 0.15.0 for Genie")
            .withTags(Sets.newHashSet("type:pig", "ver:0.15.0"))
            .withType("pig")
            .build();

        final Application hive = new Application.Builder("hive", "genieUser7", "1.0.0", ApplicationStatus.ACTIVE)
            .withDependencies(Sets.newHashSet("s3://mybucket/hive/hive-1.0.0.tar.gz"))
            .withSetupFile("s3://mybucket/hive/setup-hive.sh")
            .withConfigs(
                Sets.newHashSet("s3://mybucket/hive/hive-env.sh", "s3://mybucket/hive/hive-log4j.properties")
            )
            .withDescription("Hive 1.0.0 for Genie")
            .withTags(Sets.newHashSet("type:hive", "ver:1.0.0"))
            .withType("hive")
            .build();

        final String spark151Id = this.createConfigResource(spark151, null);
        final String spark150Id = this.createConfigResource(spark150, null);
        final String spark141Id = this.createConfigResource(spark141, null);
        final String spark140Id = this.createConfigResource(spark140, null);
        final String spark131Id = this.createConfigResource(spark131, null);
        final String pigId = this.createConfigResource(pig, null);
        final String hiveId = this.createConfigResource(hive, null);

        final List<String> appIds = Lists.newArrayList(
            spark151Id, spark150Id, spark141Id, spark140Id, spark131Id, pigId, hiveId);

        final RestDocumentationResultHandler documentationResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.APPLICATION_SEARCH_QUERY_PARAMETERS, // Request query parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response headers
            Snippets.APPLICATION_SEARCH_RESULT_FIELDS, // Result fields
            Snippets.SEARCH_LINKS // HAL Links
        );

        // Test finding all applications
        this.mvc
            .perform(MockMvcRequestBuilders.get(APPLICATIONS_API))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH, Matchers.hasSize(7)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_ID_LIST_PATH, Matchers.hasSize(7)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_ID_LIST_PATH, Matchers.containsInAnyOrder(
                spark151Id, spark150Id, spark141Id, spark140Id, spark131Id, pigId, hiveId)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_COMMANDS_LINK_PATH,
                EntitiesLinksMatcher.matchUrisAnyOrder(
                    APPLICATIONS_API, COMMANDS_LINK_KEY, COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS, appIds)))
            .andDo(documentationResultHandler);

        // Limit the size
        this.mvc
            .perform(MockMvcRequestBuilders.get(APPLICATIONS_API).param("size", "2"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH, Matchers.hasSize(2)))
            .andDo(documentationResultHandler);

        // Query by name
        this.mvc
            .perform(MockMvcRequestBuilders.get(APPLICATIONS_API).param("name", "hive"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[0].id", Matchers.is(hiveId)))
            .andDo(documentationResultHandler);

        // Query by user
        this.mvc
            .perform(MockMvcRequestBuilders.get(APPLICATIONS_API).param("user", "genieUser3"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[0].id", Matchers.is(spark141Id)))
            .andDo(documentationResultHandler);

        // Query by statuses
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .get(APPLICATIONS_API)
                    .param("status", ApplicationStatus.ACTIVE.toString(), ApplicationStatus.DEPRECATED.toString())
            )
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH, Matchers.hasSize(6)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[0].id", Matchers.is(hiveId)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[1].id", Matchers.is(pigId)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[2].id", Matchers.is(spark131Id)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[3].id", Matchers.is(spark140Id)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[4].id", Matchers.is(spark150Id)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[5].id", Matchers.is(spark151Id)))
            .andDo(documentationResultHandler);

        // Query by tags
        this.mvc
            .perform(MockMvcRequestBuilders.get(APPLICATIONS_API).param("tag", "genie.id:" + spark131Id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[0].id", Matchers.is(spark131Id)))
            .andDo(documentationResultHandler);

        // Query by type
        this.mvc
            .perform(MockMvcRequestBuilders.get(APPLICATIONS_API).param("type", "spark"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH, Matchers.hasSize(5)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[0].id", Matchers.is(spark131Id)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[1].id", Matchers.is(spark140Id)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[2].id", Matchers.is(spark141Id)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[3].id", Matchers.is(spark150Id)))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_LIST_PATH + "[4].id", Matchers.is(spark151Id)))
            .andDo(documentationResultHandler);

        //TODO: Add tests for sort, orderBy etc

        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(7L));
    }

    /**
     * Test to make sure that an application can be updated.
     *
     * @throws Exception on configuration errors
     */
    @Test
    public void canUpdateApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        final String id = this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String applicationResource = APPLICATIONS_API + "/{id}";
        final Application createdApp = this.objectMapper
            .readValue(
                this.mvc.perform(
                    MockMvcRequestBuilders.get(applicationResource, id)
                )
                    .andReturn()
                    .getResponse()
                    .getContentAsByteArray(),
                ApplicationResource.class
            ).getContent();
        Assert.assertThat(createdApp.getStatus(), Matchers.is(ApplicationStatus.ACTIVE));

        final Application.Builder newStatusApp = new Application.Builder(
            createdApp.getName(),
            createdApp.getUser(),
            createdApp.getVersion(),
            ApplicationStatus.INACTIVE
        )
            .withId(createdApp.getId().orElseThrow(IllegalArgumentException::new))
            .withCreated(createdApp.getCreated().orElseThrow(IllegalArgumentException::new))
            .withUpdated(createdApp.getUpdated().orElseThrow(IllegalArgumentException::new))
            .withTags(createdApp.getTags())
            .withConfigs(createdApp.getConfigs())
            .withDependencies(createdApp.getDependencies());

        createdApp.getDescription().ifPresent(newStatusApp::withDescription);
        createdApp.getSetupFile().ifPresent(newStatusApp::withSetupFile);

        final RestDocumentationResultHandler updateResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // request header
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.getApplicationRequestPayload() // payload fields
        );

        this.mvc.perform(
            RestDocumentationRequestBuilders
                .put(applicationResource, id)
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.objectMapper.writeValueAsBytes(newStatusApp.build()))
        )
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(updateResultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(applicationResource, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(ApplicationStatus.INACTIVE.toString())));
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(1L));
    }

    /**
     * Test to make sure that an application can be patched.
     *
     * @throws Exception on configuration errors
     */
    @Test
    public void canPatchApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        final String id = this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String applicationResource = APPLICATIONS_API + "/{id}";
        this.mvc
            .perform(MockMvcRequestBuilders.get(applicationResource, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(USER)));


        final String newUser = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/user\", \"value\": \"" + newUser + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(this.objectMapper.readTree(patchString));

        final RestDocumentationResultHandler patchResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // request headers
            Snippets.ID_PATH_PARAM, // path params
            Snippets.PATCH_FIELDS // request payload
        );

        this.mvc.perform(
            RestDocumentationRequestBuilders
                .patch(applicationResource, id)
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.objectMapper.writeValueAsBytes(patch))
        )
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(patchResultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(applicationResource, id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(newUser)));
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(1L));
    }

    /**
     * Make sure can successfully delete all applications.
     *
     * @throws Exception on a configuration error
     */
    @Test
    public void canDeleteAllApplications() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).build(),
            null
        );
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.DEPRECATED).build(),
            null
        );
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.INACTIVE).build(),
            null
        );
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(3L));

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint())
        );

        this.mvc
            .perform(MockMvcRequestBuilders.delete(APPLICATIONS_API))
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(deleteResultHandler);

        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
    }

    /**
     * Test to make sure that you can delete an application.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canDeleteAnApplication() throws Exception {
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


        this.createConfigResource(
            new Application
                .Builder(name1, user1, version1, ApplicationStatus.ACTIVE)
                .withId(id1)
                .build(),
            null
        );
        this.createConfigResource(
            new Application
                .Builder(name2, user2, version2, ApplicationStatus.DEPRECATED)
                .withId(id2)
                .build(),
            null
        );
        this.createConfigResource(
            new Application
                .Builder(name3, user3, version3, ApplicationStatus.INACTIVE)
                .withId(id3)
                .build(),
            null
        );
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(3L));

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM // path parameters
        );

        this.mvc
            .perform(RestDocumentationRequestBuilders.delete(APPLICATIONS_API + "/{id}", id2))
            .andExpect(MockMvcResultMatchers.status().isNoContent())
            .andDo(deleteResultHandler);

        this.mvc
            .perform(MockMvcRequestBuilders.get(APPLICATIONS_API + "/{id}", id2))
            .andExpect(MockMvcResultMatchers.status().isNotFound());

        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(2L));
    }

    /**
     * Test to make sure we can add configurations to the application after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canAddConfigsToApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );

        final RestDocumentationResultHandler addResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // request headers
            Snippets.ID_PATH_PARAM, // path parameters
            PayloadDocumentation.requestFields(Snippets.CONFIG_FIELDS) // request payload fields
        );
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.JSON_CONTENT_TYPE_HEADER, // response headers
            PayloadDocumentation.responseFields(Snippets.CONFIG_FIELDS) // response fields
        );
        this.canAddElementsToResource(APPLICATIONS_API + "/{id}/configs", ID, addResultHandler, getResultHandler);
    }

    /**
     * Test to make sure we can update the configurations for an application after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canUpdateConfigsForApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );

        final RestDocumentationResultHandler updateResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.CONFIG_FIELDS) // Request fields
        );
        this.canUpdateElementsForResource(APPLICATIONS_API + "/{id}/configs", ID, updateResultHandler);
    }

    /**
     * Test to make sure we can delete the configurations for an application after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteConfigsForApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM // Path parameters
        );
        this.canDeleteElementsFromResource(APPLICATIONS_API + "/{id}/configs", ID, deleteResultHandler);
    }

    /**
     * Test to make sure we can add dependencies to the application after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canAddDependenciesToApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );

        final RestDocumentationResultHandler addResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // path params
            Snippets.CONTENT_TYPE_HEADER, // request header
            PayloadDocumentation.requestFields(Snippets.DEPENDENCIES_FIELDS) // response fields
        );
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // path params
            Snippets.JSON_CONTENT_TYPE_HEADER, // response headers
            PayloadDocumentation.responseFields(Snippets.DEPENDENCIES_FIELDS) // response fields
        );
        this.canAddElementsToResource(
            APPLICATIONS_API + "/{id}/dependencies",
            ID,
            addResultHandler,
            getResultHandler
        );
    }

    /**
     * Test to make sure we can update the dependencies for an application after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canUpdateDependenciesForApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );

        final RestDocumentationResultHandler updateResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.DEPENDENCIES_FIELDS) // Request fields
        );
        this.canUpdateElementsForResource(
            APPLICATIONS_API + "/{id}/dependencies",
            ID,
            updateResultHandler
        );
    }

    /**
     * Test to make sure we can delete the dependencies for an application after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteDependenciesForApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM // Path variables
        );
        this.canDeleteElementsFromResource(
            APPLICATIONS_API + "/{id}/dependencies",
            ID,
            deleteResultHandler
        );
    }

    /**
     * Test to make sure we can add tags to the application after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canAddTagsToApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String api = APPLICATIONS_API + "/{id}/tags";

        final RestDocumentationResultHandler addResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // path params
            Snippets.CONTENT_TYPE_HEADER, // request header
            PayloadDocumentation.requestFields(Snippets.TAGS_FIELDS) // response fields
        );
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // path parameters
            Snippets.JSON_CONTENT_TYPE_HEADER, // response headers
            PayloadDocumentation.responseFields(Snippets.TAGS_FIELDS) // response fields
        );
        this.canAddTagsToResource(api, ID, NAME, addResultHandler, getResultHandler);
    }

    /**
     * Test to make sure we can update the tags for an application after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canUpdateTagsForApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String api = APPLICATIONS_API + "/{id}/tags";

        final RestDocumentationResultHandler updateResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.CONTENT_TYPE_HEADER, // Request header
            Snippets.ID_PATH_PARAM, // Path parameters
            PayloadDocumentation.requestFields(Snippets.TAGS_FIELDS) // Request fields
        );
        this.canUpdateTagsForResource(api, ID, NAME, updateResultHandler);
    }

    /**
     * Test to make sure we can delete the tags for an application after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteTagsForApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String api = APPLICATIONS_API + "/{id}/tags";

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM
        );
        this.canDeleteTagsForResource(api, ID, NAME, deleteResultHandler);
    }

    /**
     * Test to make sure we can delete a tag for an application after it is created.
     *
     * @throws Exception on configuration problems
     */
    @Test
    public void canDeleteTagForApplication() throws Exception {
        Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String api = APPLICATIONS_API + "/{id}/tags";

        final RestDocumentationResultHandler deleteResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM.and(
                RequestDocumentation.parameterWithName("tag").description("The tag to remove")
            )
        );
        this.canDeleteTagForResource(api, ID, NAME, deleteResultHandler);
    }

    /**
     * Make sure can get all the commands which use a given application.
     *
     * @throws Exception on configuration error
     */
    @Test
    public void canGetCommandsForApplication() throws Exception {
        this.createConfigResource(
            new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).withId(ID).build(),
            null
        );
        final String placeholder = UUID.randomUUID().toString();
        final String command1Id = UUID.randomUUID().toString();
        final String command2Id = UUID.randomUUID().toString();
        final String command3Id = UUID.randomUUID().toString();
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.ACTIVE, placeholder, 1000L)
                .withId(command1Id)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.INACTIVE, placeholder, 1100L)
                .withId(command2Id)
                .build(),
            null
        );
        this.createConfigResource(
            new Command
                .Builder(placeholder, placeholder, placeholder, CommandStatus.DEPRECATED, placeholder, 1200L)
                .withId(command3Id)
                .build(),
            null
        );

        final Set<String> appIds = Sets.newHashSet(ID);
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(COMMANDS_API + "/" + command1Id + "/applications")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(appIds))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(COMMANDS_API + "/" + command3Id + "/applications")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(appIds))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        final String applicationCommandsAPI = APPLICATIONS_API + "/{id}/commands";

        Arrays.asList(
            this.objectMapper.readValue(
                this.mvc
                    .perform(MockMvcRequestBuilders.get(applicationCommandsAPI, ID))
                    .andExpect(MockMvcResultMatchers.status().isOk())
                    .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
                    .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
                    .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
                    .andReturn()
                    .getResponse()
                    .getContentAsByteArray(),
                Command[].class
            )
        ).forEach(
            command -> {
                if (!command.getId().orElseThrow(IllegalArgumentException::new).equals(command1Id)
                    && !command.getId().orElseThrow(IllegalArgumentException::new).equals(command3Id)) {
                    Assert.fail();
                }
            }
        );

        // Filter by status
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/{method-name}/{step}/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // Path parameters
            RequestDocumentation.requestParameters(
                RequestDocumentation
                    .parameterWithName("status")
                    .description("The status of commands to search for")
                    .attributes(
                        Attributes.key(Snippets.CONSTRAINTS).value(CommandStatus.values())
                    )
                    .optional()
            ), // Query Parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            PayloadDocumentation.responseFields(
                PayloadDocumentation
                    .fieldWithPath("[]")
                    .description("The list of commands found")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            )
        );
        this.mvc
            .perform(
                RestDocumentationRequestBuilders
                    .get(applicationCommandsAPI, ID)
                    .param("status", CommandStatus.ACTIVE.toString(), CommandStatus.INACTIVE.toString())
            )
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(command1Id)))
            .andDo(getResultHandler);
    }

    /**
     * Test creating an application with blank files and tag resources.
     * @throws Exception when an unexpected error is encountered
     */
    @Test
    public void cantCreateApplicationWithBlankFields() throws Exception {

        final Set<String> stringSetWithBlank = Sets.newHashSet("foo", " ");

        final List<Application> invalidApplicationResources = Lists.newArrayList(
            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withId(ID)
                .withSetupFile(" ")
                .build(),

            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withId(ID)
                .withConfigs(stringSetWithBlank)
                .build(),

            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withId(ID)
                .withDependencies(stringSetWithBlank)
                .build(),

            new Application
                .Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE)
                .withId(ID)
                .withTags(stringSetWithBlank)
                .build()
        );

        for (Application invalidApplicationResource : invalidApplicationResources) {
            Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));

            this.mvc
                .perform(
                    MockMvcRequestBuilders.post(APPLICATIONS_API)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(this.objectMapper.writeValueAsBytes(invalidApplicationResource))
                )
                .andExpect(MockMvcResultMatchers.status().isPreconditionFailed());

            Assert.assertThat(this.jpaApplicationRepository.count(), Matchers.is(0L));
        }
    }
}
