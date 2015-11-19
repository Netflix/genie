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
package com.netflix.genie.docs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.GenieWeb;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.test.categories.DocumentationTest;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.restdocs.RestDocumentation;
import org.springframework.restdocs.constraints.ConstraintDescriptions;
import org.springframework.restdocs.hypermedia.HypermediaDocumentation;
import org.springframework.restdocs.mockmvc.MockMvcRestDocumentation;
import org.springframework.restdocs.mockmvc.RestDocumentationResultHandler;
import org.springframework.restdocs.operation.preprocess.Preprocessors;
import org.springframework.restdocs.payload.FieldDescriptor;
import org.springframework.restdocs.payload.JsonFieldType;
import org.springframework.restdocs.payload.PayloadDocumentation;
import org.springframework.restdocs.request.RequestDocumentation;
import org.springframework.restdocs.snippet.Attributes;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.util.StringUtils;
import org.springframework.web.context.WebApplicationContext;

import javax.servlet.RequestDispatcher;
import java.util.List;

/**
 * Used to generate the documentation for the Genie REST APIs.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(DocumentationTest.class)
@ActiveProfiles({"docs"})
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = GenieWeb.class)
@WebIntegrationTest(randomPort = true)
public class ApplicationRestDocs {

    private static final String APPLICATION_API_PATH = "/api/v3/applications";
    /**
     * Where to put the generated documentation.
     */
    @Rule
    public final RestDocumentation restDocumentation = new RestDocumentation("build/generated-snippets");

    @Autowired
    private WebApplicationContext context;

    @Autowired
    private JpaApplicationRepository jpaApplicationRepository;

    private RestDocumentationResultHandler document;
    private MockMvc mockMvc;

    /**
     * Setup the tests.
     */
    @Before
    public void setUp() {
        this.document = MockMvcRestDocumentation.document(
                "{method-name}",
                Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
                Preprocessors.preprocessResponse(Preprocessors.prettyPrint())
        );

        this.mockMvc = MockMvcBuilders
                .webAppContextSetup(this.context)
                .apply(MockMvcRestDocumentation.documentationConfiguration(this.restDocumentation))
                .build();
    }

    /**
     * Reset for the next test.
     */
    @After
    public void teardown() {
        this.jpaApplicationRepository.deleteAll();
    }

    /**
     * Test to document error responses.
     *
     * @throws Exception For any error
     */
    @Test
    public void errorExample() throws Exception {
        this.document.snippets(
                PayloadDocumentation.responseFields(
                        PayloadDocumentation
                                .fieldWithPath("error")
                                .description("The HTTP error that occurred, e.g. `Bad Request`"),
                        PayloadDocumentation
                                .fieldWithPath("message")
                                .description("A description of the cause of the error"),
                        PayloadDocumentation
                                .fieldWithPath("path")
                                .description("The path to which the request was made"),
                        PayloadDocumentation
                                .fieldWithPath("status")
                                .description("The HTTP status code, e.g. `400`"),
                        PayloadDocumentation
                                .fieldWithPath("timestamp")
                                .description("The time, in milliseconds, at which the error occurred")
                )
        );

        this.mockMvc
                .perform(MockMvcRequestBuilders
                                .get("/error")
                                .requestAttr(RequestDispatcher.ERROR_STATUS_CODE, 404)
                                .requestAttr(RequestDispatcher.ERROR_REQUEST_URI, APPLICATION_API_PATH + "/1")
                                .requestAttr(
                                        RequestDispatcher.ERROR_MESSAGE,
                                        "The application 'http://localhost:8080"
                                                + APPLICATION_API_PATH
                                                + "/1' does not exist"
                                )
                )
                .andDo(this.document)
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isNotFound())
                .andExpect(MockMvcResultMatchers.jsonPath("error", Matchers.is("Not Found")))
                .andExpect(MockMvcResultMatchers.jsonPath("timestamp", Matchers.is(Matchers.notNullValue())))
                .andExpect(MockMvcResultMatchers.jsonPath("status", Matchers.is(404)))
                .andExpect(MockMvcResultMatchers.jsonPath("path", Matchers.is(Matchers.notNullValue())));
    }

    /**
     * Document the creation process for applications.
     *
     * @throws Exception For any error
     */
    @Test
    public void createApplication() throws Exception {
        final Application app = new Application.Builder("spark", "genieUser", "1.5.1", ApplicationStatus.ACTIVE)
                .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.5.1.tar.gz"))
                .withSetupFile("s3://mybucket/spark/setup-spark.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
                .withDescription("Spark 1.5.1 for Genie")
                .withTags(Sets.newHashSet("type:spark", "ver:1.5.1"))
                .build();

        this.document.snippets(
                PayloadDocumentation.requestFields(this.getApplicationFieldDescriptors(false))
                //TODO: Add header documentation once RESTDocs supports it (in snapshot right now 10/5/15)
        );

        this.mockMvc
                .perform(
                        MockMvcRequestBuilders.post(APPLICATION_API_PATH)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(new ObjectMapper().writeValueAsBytes(app))
                )
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andExpect(MockMvcResultMatchers
                                .header()
                                .string(HttpHeaders.LOCATION, Matchers.is(Matchers.notNullValue()))
                )
                .andDo(this.document);
    }

    /**
     * Document getting an application.
     *
     * @throws Exception For any error
     */
    @Test
    public void getApplication() throws Exception {
        final Application app = new Application.Builder("spark", "genieUser", "1.5.1", ApplicationStatus.ACTIVE)
                .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.5.1.tar.gz"))
                .withSetupFile("s3://mybucket/spark/setup-spark.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
                .withDescription("Spark 1.5.1 for Genie")
                .withTags(Sets.newHashSet("type:spark", "ver:1.5.1"))
                .build();

        final String location = this.mockMvc
                .perform(
                        MockMvcRequestBuilders.post(APPLICATION_API_PATH)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(new ObjectMapper().writeValueAsBytes(app))
                )
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andExpect(MockMvcResultMatchers
                                .header()
                                .string(HttpHeaders.LOCATION, Matchers.is(Matchers.notNullValue()))
                ).andReturn().getResponse().getHeader(HttpHeaders.LOCATION);

        this.document.snippets(
                HypermediaDocumentation.links(
                        HypermediaDocumentation.linkWithRel("self").description("Location of the application"),
                        HypermediaDocumentation.linkWithRel("commands").description("The commands for the application")
                ),
                PayloadDocumentation.responseFields(this.getApplicationFieldDescriptors(true))
        );

        this.mockMvc
                .perform(MockMvcRequestBuilders.get(location).accept(MediaTypes.HAL_JSON))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(this.document);
    }

    /**
     * Document searching for applications.
     *
     * @throws Exception For any error
     */
    @Test
    public void findApplications() throws Exception {
        final Application spark151 = new Application.Builder("spark", "genieUser", "1.5.1", ApplicationStatus.ACTIVE)
                .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.5.1.tar.gz"))
                .withSetupFile("s3://mybucket/spark/setup-spark.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
                .withDescription("Spark 1.5.1 for Genie")
                .withTags(Sets.newHashSet("type:spark", "ver:1.5.1"))
                .build();

        final Application spark150 = new Application.Builder("spark", "genieUser", "1.5.0", ApplicationStatus.ACTIVE)
                .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.5.0.tar.gz"))
                .withSetupFile("s3://mybucket/spark/setup-spark.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
                .withDescription("Spark 1.5.0 for Genie")
                .withTags(Sets.newHashSet("type:spark", "ver:1.5.0"))
                .build();

        final Application spark141 = new Application.Builder("spark", "genieUser", "1.4.1", ApplicationStatus.ACTIVE)
                .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.4.1.tar.gz"))
                .withSetupFile("s3://mybucket/spark/setup-spark.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
                .withDescription("Spark 1.4.1 for Genie")
                .withTags(Sets.newHashSet("type:spark", "ver:1.4.1"))
                .build();

        final Application spark140 = new Application.Builder("spark", "genieUser", "1.4.0", ApplicationStatus.ACTIVE)
                .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.4.0.tar.gz"))
                .withSetupFile("s3://mybucket/spark/setup-spark.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
                .withDescription("Spark 1.4.0 for Genie")
                .withTags(Sets.newHashSet("type:spark", "ver:1.4.0"))
                .build();

        final Application spark131 = new Application.Builder("spark", "genieUser", "1.3.1", ApplicationStatus.ACTIVE)
                .withDependencies(Sets.newHashSet("s3://mybucket/spark/spark-1.3.1.tar.gz"))
                .withSetupFile("s3://mybucket/spark/setup-spark.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/spark/spark-env.sh"))
                .withDescription("Spark 1.3.1 for Genie")
                .withTags(Sets.newHashSet("type:spark", "ver:1.3.1"))
                .build();

        final Application pig = new Application.Builder("spark", "genieUser", "0.4.0", ApplicationStatus.ACTIVE)
                .withDependencies(Sets.newHashSet("s3://mybucket/pig/pig-0.15.0.tar.gz"))
                .withSetupFile("s3://mybucket/pig/setup-pig.sh")
                .withConfigs(Sets.newHashSet("s3://mybucket/pig/pig.properties"))
                .withDescription("Pig 0.15.0 for Genie")
                .withTags(Sets.newHashSet("type:pig", "ver:0.15.0"))
                .build();

        final Application hive = new Application.Builder("hive", "genieUser", "1.0.0", ApplicationStatus.ACTIVE)
                .withDependencies(Sets.newHashSet("s3://mybucket/hive/hive-1.0.0.tar.gz"))
                .withSetupFile("s3://mybucket/hive/setup-hive.sh")
                .withConfigs(
                        Sets.newHashSet("s3://mybucket/hive/hive-env.sh", "s3://mybucket/hive/hive-log4j.properties")
                )
                .withDescription("Hive 1.0.0 for Genie")
                .withTags(Sets.newHashSet("type:hive", "ver:1.0.0"))
                .build();

        this.mockMvc
                .perform(
                        MockMvcRequestBuilders.post(APPLICATION_API_PATH)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(new ObjectMapper().writeValueAsBytes(spark151))
                )
                .andExpect(MockMvcResultMatchers.status().isCreated());

        this.mockMvc
                .perform(
                        MockMvcRequestBuilders.post(APPLICATION_API_PATH)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(new ObjectMapper().writeValueAsBytes(spark150))
                )
                .andExpect(MockMvcResultMatchers.status().isCreated());

        this.mockMvc
                .perform(
                        MockMvcRequestBuilders.post(APPLICATION_API_PATH)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(new ObjectMapper().writeValueAsBytes(spark141))
                )
                .andExpect(MockMvcResultMatchers.status().isCreated());

        this.mockMvc
                .perform(
                        MockMvcRequestBuilders.post(APPLICATION_API_PATH)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(new ObjectMapper().writeValueAsBytes(spark140))
                )
                .andExpect(MockMvcResultMatchers.status().isCreated());

        this.mockMvc
                .perform(
                        MockMvcRequestBuilders.post(APPLICATION_API_PATH)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(new ObjectMapper().writeValueAsBytes(spark131))
                )
                .andExpect(MockMvcResultMatchers.status().isCreated());

        this.mockMvc
                .perform(
                        MockMvcRequestBuilders.post(APPLICATION_API_PATH)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(new ObjectMapper().writeValueAsBytes(pig))
                )
                .andExpect(MockMvcResultMatchers.status().isCreated());

        this.mockMvc
                .perform(
                        MockMvcRequestBuilders.post(APPLICATION_API_PATH)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(new ObjectMapper().writeValueAsBytes(hive))
                )
                .andExpect(MockMvcResultMatchers.status().isCreated());

        this.document.snippets(
                HypermediaDocumentation.links(
                        HypermediaDocumentation.linkWithRel("self").description("The current search"),
                        HypermediaDocumentation.linkWithRel("first").description("The first page for this search"),
                        HypermediaDocumentation.linkWithRel("prev").description("The previous page for this search"),
                        HypermediaDocumentation.linkWithRel("next").description("The next page for this search"),
                        HypermediaDocumentation.linkWithRel("last").description("The last page for this search")
                ),
                RequestDocumentation.requestParameters(
                        RequestDocumentation
                                .parameterWithName("name")
                                .description("The name of the applications to find."),
                        RequestDocumentation
                                .parameterWithName("userName")
                                .description("The user of the applications to find."),
                        RequestDocumentation
                                .parameterWithName("status")
                                .description("The status of the applications to find."),
                        RequestDocumentation
                                .parameterWithName("tag")
                                .description("The tag(s) of the applications to find."),
                        RequestDocumentation
                                .parameterWithName("page")
                                .description("The page number to get. Default to 0."),
                        RequestDocumentation
                                .parameterWithName("size")
                                .description("The size of the page to get. Default to 64."),
                        RequestDocumentation
                                .parameterWithName("sort")
                                .description("The fields to sort the results by. Defaults to 'updated,desc'.")
                ),
                PayloadDocumentation.responseFields(
                        PayloadDocumentation
                                .fieldWithPath("_embedded.applicationList")
                                .description("The found applications."),
                        PayloadDocumentation
                                .fieldWithPath("_links")
                                .description("<<resources-index-links,Links>> to other resources."),
                        PayloadDocumentation
                                .fieldWithPath("page")
                                .description("The result page information."),
                        PayloadDocumentation
                                .fieldWithPath("page.size")
                                .description("The number of elements in this page result."),
                        PayloadDocumentation
                                .fieldWithPath("page.totalElements")
                                .description("The total number of elements this search result could return."),
                        PayloadDocumentation
                                .fieldWithPath("page.totalPages")
                                .description("The total number of pages there could be at the current page size."),
                        PayloadDocumentation
                                .fieldWithPath("page.number")
                                .description("The current page number.")
                )
        );

        this.mockMvc
                .perform(MockMvcRequestBuilders
                                .get(APPLICATION_API_PATH
                                        + "?name=spark&userName=genieUser&status=ACTIVE"
                                        + "&tag=type:spark&page=1&size=2&sort=updated,desc")
                                .accept(MediaTypes.HAL_JSON)
                )
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(this.document);
    }

    private FieldDescriptor[] getApplicationFieldDescriptors(final boolean addLinks) {
        final ConstraintDescriptions constraintDescriptions = new ConstraintDescriptions(Application.class);
        final List<FieldDescriptor> descriptors = Lists.newArrayList(
                PayloadDocumentation
                        .fieldWithPath("id")
                        .type(JsonFieldType.STRING)
                        .attributes(this.getConstraintsForField(constraintDescriptions, "id"))
                        .description("The id of the application. If not set the system will set one.")
                        .optional(),
                PayloadDocumentation
                        .fieldWithPath("created")
                        .type(JsonFieldType.STRING)
                        .attributes(this.getConstraintsForField(constraintDescriptions, "created"))
                        .description("The time the application was last created. Set by system.")
                        .optional(),
                PayloadDocumentation
                        .fieldWithPath("updated")
                        .type(JsonFieldType.STRING)
                        .attributes(this.getConstraintsForField(constraintDescriptions, "updated"))
                        .description("The time the application was last updated. Set by system.")
                        .optional(),
                PayloadDocumentation
                        .fieldWithPath("name")
                        .attributes(this.getConstraintsForField(constraintDescriptions, "name"))
                        .description("The name of the application"),
                PayloadDocumentation
                        .fieldWithPath("user")
                        .attributes(this.getConstraintsForField(constraintDescriptions, "user"))
                        .description("The user who created the application"),
                PayloadDocumentation
                        .fieldWithPath("version")
                        .attributes(this.getConstraintsForField(constraintDescriptions, "version"))
                        .description("The version of the application"),
                PayloadDocumentation
                        .fieldWithPath("description")
                        .type(JsonFieldType.STRING)
                        .attributes(this.getConstraintsForField(constraintDescriptions, "description"))
                        .description("Any description for the application.")
                        .optional(),
                PayloadDocumentation
                        .fieldWithPath("status")
                        .attributes(this.getConstraintsForField(constraintDescriptions, "status"))
                        .description("The status of the application"),
                PayloadDocumentation
                        .fieldWithPath("tags")
                        .type(JsonFieldType.ARRAY)
                        .attributes(this.getConstraintsForField(constraintDescriptions, "tags"))
                        .description("The tags for the application")
                        .optional(),
                PayloadDocumentation
                        .fieldWithPath("dependencies")
                        .type(JsonFieldType.ARRAY)
                        .attributes(this.getConstraintsForField(constraintDescriptions, "dependencies"))
                        .description("The dependencies for the application")
                        .optional(),
                PayloadDocumentation
                        .fieldWithPath("setupFile")
                        .type(JsonFieldType.STRING)
                        .attributes(this.getConstraintsForField(constraintDescriptions, "setupFile"))
                        .description("A location for any setup that needs to be done when installing")
                        .optional(),
                PayloadDocumentation
                        .fieldWithPath("configs")
                        .type(JsonFieldType.ARRAY)
                        .attributes(this.getConstraintsForField(constraintDescriptions, "configs"))
                        .description("Any configuration files needed for the application")
                        .optional()
        );

        if (addLinks) {
            descriptors.add(
                    PayloadDocumentation
                            .fieldWithPath("_links")
                            .description("<<resources-index-links,Links>> to other resources")
            );
        }

        return descriptors.toArray(new FieldDescriptor[descriptors.size()]);
    }

    private Attributes.Attribute getConstraintsForField(
            final ConstraintDescriptions constraints,
            final String fieldName
    ) {
        return Attributes
                .key("constraints")
                .value(
                        StringUtils.collectionToDelimitedString(
                                constraints.descriptionsForProperty(fieldName),
                                ". "
                        )
                );
    }
}
