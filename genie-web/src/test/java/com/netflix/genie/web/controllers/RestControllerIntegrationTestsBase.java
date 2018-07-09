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

import com.google.common.collect.Sets;
import com.netflix.genie.GenieTestApp;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.ExecutionEnvironmentDTO;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.specification.RequestSpecification;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.cloud.contract.wiremock.restdocs.WireMockSnippet;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.restdocs.JUnitRestDocumentation;
import org.springframework.restdocs.operation.preprocess.Preprocessors;
import org.springframework.restdocs.restassured3.RestAssuredRestDocumentation;
import org.springframework.restdocs.restassured3.RestDocumentationFilter;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Base class for all integration tests for the controllers.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(classes = GenieTestApp.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(resolver = IntegrationTestActiveProfilesResolver.class)
public abstract class RestControllerIntegrationTestsBase {

    static final String APPLICATIONS_API = "/api/v3/applications";
    static final String CLUSTERS_API = "/api/v3/clusters";
    static final String COMMANDS_API = "/api/v3/commands";
    static final String JOBS_API = "/api/v3/jobs";
    static final String ID_PATH = "id";
    static final String CREATED_PATH = "created";
    static final String UPDATED_PATH = "updated";
    static final String NAME_PATH = "name";
    static final String VERSION_PATH = "version";
    static final String USER_PATH = "user";
    static final String DESCRIPTION_PATH = "description";
    static final String METADATA_PATH = "metadata";
    static final String TAGS_PATH = "tags";
    static final String SETUP_FILE_PATH = "setupFile";
    static final String STATUS_PATH = "status";
    static final String CONFIGS_PATH = "configs";
    static final String DEPENDENCIES_PATH = "dependencies";
    static final String LINKS_PATH = "_links";
    static final String EMBEDDED_PATH = "_embedded";
    // Link Keys
    static final String SELF_LINK_KEY = "self";
    static final String COMMANDS_LINK_KEY = "commands";
    static final String CLUSTERS_LINK_KEY = "clusters";
    static final String APPLICATIONS_LINK_KEY = "applications";
    static final String JOBS_LINK_KEY = "jobs";
    static final Set<String> CLUSTERS_OPTIONAL_HAL_LINK_PARAMETERS = Sets.newHashSet("status");
    static final Set<String> COMMANDS_OPTIONAL_HAL_LINK_PARAMETERS = Sets.newHashSet("status");
    private static final String URI_HOST = "genie.example.com";
    private static final String URI_SCHEME = "https";

    /**
     * Setup for the Spring Rest Docs wiring.
     */
    @Rule
    public final JUnitRestDocumentation restDocumentation = new JUnitRestDocumentation();

    @LocalServerPort
    protected int port;

    @Autowired
    protected JpaApplicationRepository applicationRepository;

    @Autowired
    protected JpaClusterRepository clusterRepository;

    @Autowired
    protected JpaCommandRepository commandRepository;

    @Autowired
    protected JpaJobRepository jobRepository;

    @Autowired
    protected JpaFileRepository fileRepository;

    @Autowired
    protected JpaTagRepository tagRepository;

    @Getter
    private RequestSpecification requestSpecification;

    private static String getLinkedResourceExpectedUri(
        final String entityApi,
        final String entityId,
        final Set<String> optionalHalParams,
        final String linkedEntityType
    ) {
        final String uriPath = entityApi + "/" + entityId + "/" + linkedEntityType;

        // Append HAL parameters separately to avoid URI encoding
        final StringBuilder halParamsStringBuilder = new StringBuilder();
        if (optionalHalParams != null && !optionalHalParams.isEmpty()) {
            halParamsStringBuilder
                .append("{?")
                .append(StringUtils.join(optionalHalParams, ","))
                .append("}");
        }
        return uriPath + halParamsStringBuilder.toString();
    }

    /**
     * Clean out the db before every test.
     *
     * @throws Exception on error
     */
    public void setup() throws Exception {
        this.jobRepository.deleteAll();
        this.clusterRepository.deleteAll();
        this.commandRepository.deleteAll();
        this.applicationRepository.deleteAll();
        this.fileRepository.deleteAll();
        this.tagRepository.deleteAll();

        this.requestSpecification = new RequestSpecBuilder()
            .addFilter(
                RestAssuredRestDocumentation
                    .documentationConfiguration(this.restDocumentation)
                    .snippets().withAdditionalDefaults(new WireMockSnippet())
                    .and()
                    .operationPreprocessors()
                    .withRequestDefaults(
                        Preprocessors.prettyPrint(),
                        Preprocessors.modifyUris().scheme(URI_SCHEME).host(URI_HOST).removePort()
                    )
                    .withResponseDefaults(
                        Preprocessors.prettyPrint(),
                        Preprocessors.modifyUris().host(URI_HOST).scheme(URI_SCHEME).removePort()
                    )
            )
            .build();
    }

    /**
     * Clean out the db after every test.
     *
     * @throws Exception on error
     */
    public void cleanup() throws Exception {
        this.jobRepository.deleteAll();
        this.clusterRepository.deleteAll();
        this.commandRepository.deleteAll();
        this.applicationRepository.deleteAll();
        this.fileRepository.deleteAll();
        this.tagRepository.deleteAll();
    }

    void canAddElementsToResource(
        final String api,
        final String id,
        final RestDocumentationFilter addFilter,
        final RestDocumentationFilter getFilter
    ) throws Exception {
        RestAssured
            .given(this.requestSpecification)
            .when()
            .port(this.port)
            .get(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .body("$", Matchers.empty());

        final String element1 = UUID.randomUUID().toString();
        final String element2 = UUID.randomUUID().toString();
        final Set<String> elements = Sets.newHashSet(element1, element2);

        RestAssured
            .given(this.requestSpecification)
            .filter(addFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(elements))
            .when()
            .port(this.port)
            .post(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.requestSpecification)
            .filter(getFilter)
            .when()
            .port(this.port)
            .get(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(2))
            .body("$", Matchers.hasItem(element1))
            .body("$", Matchers.hasItem(element2));
    }

    void canUpdateElementsForResource(
        final String api,
        final String id,
        final RestDocumentationFilter updateFilter
    ) throws Exception {
        final String element1 = UUID.randomUUID().toString();
        final String element2 = UUID.randomUUID().toString();
        final Set<String> elements = Sets.newHashSet(element1, element2);

        RestAssured
            .given(this.requestSpecification)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(elements))
            .when()
            .port(this.port)
            .post(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        final String element3 = UUID.randomUUID().toString();
        RestAssured
            .given(this.requestSpecification)
            .filter(updateFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(Sets.newHashSet(element3)))
            .when()
            .port(this.port)
            .put(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.requestSpecification)
            .when()
            .port(this.port)
            .get(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(1))
            .body("$", Matchers.hasItem(element3));
    }

    void canDeleteElementsFromResource(
        final String api,
        final String id,
        final RestDocumentationFilter deleteFilter
    ) throws Exception {
        final String element1 = UUID.randomUUID().toString();
        final String element2 = UUID.randomUUID().toString();

        RestAssured
            .given(this.requestSpecification)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(Sets.newHashSet(element1, element2)))
            .when()
            .port(this.port)
            .post(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.requestSpecification)
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.requestSpecification)
            .when()
            .port(this.port)
            .get(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .body("$", Matchers.empty());
    }

    void canAddTagsToResource(
        final String api,
        final String id,
        final String name,
        final RestDocumentationFilter addFilter,
        final RestDocumentationFilter getFilter
    ) throws Exception {
        RestAssured
            .given(this.requestSpecification)
            .when()
            .port(this.port)
            .get(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(2))
            .body("$", Matchers.hasItem("genie.id:" + id))
            .body("$", Matchers.hasItem("genie.name:" + name));

        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final Set<String> tags = Sets.newHashSet(tag1, tag2);

        RestAssured
            .given(this.requestSpecification)
            .filter(addFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(tags))
            .when()
            .port(this.port)
            .post(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.requestSpecification)
            .filter(getFilter)
            .when()
            .port(this.port)
            .get(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(4))
            .body("$", Matchers.hasItem("genie.id:" + id))
            .body("$", Matchers.hasItem("genie.name:" + name))
            .body("$", Matchers.hasItem(tag1))
            .body("$", Matchers.hasItem(tag2));
    }

    void canUpdateTagsForResource(
        final String api,
        final String id,
        final String name,
        final RestDocumentationFilter updateFilter
    ) throws Exception {
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final Set<String> tags = Sets.newHashSet(tag1, tag2);

        RestAssured
            .given(this.requestSpecification)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(tags))
            .when()
            .port(this.port)
            .post(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        final String tag3 = UUID.randomUUID().toString();
        RestAssured
            .given(this.requestSpecification)
            .filter(updateFilter)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(Sets.newHashSet(tag3)))
            .when()
            .port(this.port)
            .put(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.requestSpecification)
            .when()
            .port(this.port)
            .get(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(3))
            .body("$", Matchers.hasItem("genie.id:" + id))
            .body("$", Matchers.hasItem("genie.name:" + name))
            .body("$", Matchers.hasItem(tag3));
    }

    void canDeleteTagsForResource(
        final String api,
        final String id,
        final String name,
        final RestDocumentationFilter deleteFilter
    ) throws Exception {
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final Set<String> tags = Sets.newHashSet(tag1, tag2);

        RestAssured
            .given(this.requestSpecification)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(tags))
            .when()
            .port(this.port)
            .post(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.requestSpecification)
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.requestSpecification)
            .when()
            .port(this.port)
            .get(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(2))
            .body("$", Matchers.hasItem("genie.id:" + id))
            .body("$", Matchers.hasItem("genie.name:" + name));
    }

    void canDeleteTagForResource(
        final String api,
        final String id,
        final String name,
        final RestDocumentationFilter deleteFilter
    ) throws Exception {
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();

        RestAssured
            .given(this.requestSpecification)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(Sets.newHashSet(tag1, tag2)))
            .when()
            .port(this.port)
            .post(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.requestSpecification)
            .filter(deleteFilter)
            .when()
            .port(this.port)
            .delete(api + "/{tag}", id, tag1)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));

        RestAssured
            .given(this.requestSpecification)
            .when()
            .port(this.port)
            .get(api, id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .body("$", Matchers.hasSize(3))
            .body("$", Matchers.hasItem("genie.id:" + id))
            .body("$", Matchers.hasItem("genie.name:" + name))
            .body("$", Matchers.hasItem(tag2));
    }

    <R extends ExecutionEnvironmentDTO> String createConfigResource(
        @NotNull final R resource,
        @Nullable final RestDocumentationFilter documentationFilter
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

        final RequestSpecification configRequestSpec = RestAssured
            .given(this.requestSpecification)
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(resource));

        if (documentationFilter != null) {
            configRequestSpec.filter(documentationFilter);
        }

        return this.getIdFromLocation(
            configRequestSpec
                .when()
                .port(this.port)
                .post(endpoint)
                .then()
                .statusCode(Matchers.is(HttpStatus.CREATED.value()))
                .header(HttpHeaders.LOCATION, Matchers.notNullValue())
                .extract()
                .header(HttpHeaders.LOCATION)
        );
    }

    String getIdFromLocation(@Nullable final String location) {
        if (location == null) {
            Assert.fail();
        }
        return location.substring(location.lastIndexOf("/") + 1);
    }

    static class EntityLinkMatcher extends TypeSafeMatcher<String> {

        private final String expectedUri;
        private String descriptionString = "Not evaluated";

        EntityLinkMatcher(final String expectedUri) {
            this.expectedUri = expectedUri;
        }

        static EntityLinkMatcher matchUri(
            final String entityApi,
            final String linkedEntityKey,
            final Set<String> optionalHalParams,
            final String entityId
        ) {
            return new EntityLinkMatcher(
                getLinkedResourceExpectedUri(entityApi, entityId, optionalHalParams, linkedEntityKey)
            );
        }

        @Override
        protected boolean matchesSafely(final String actualUrl) {
            if (!actualUrl.endsWith(this.expectedUri)) {
                this.descriptionString = "Expected to end with: " + this.expectedUri + " got: " + actualUrl;
                return false;
            } else {
                this.descriptionString = "Successfully matched: " + this.expectedUri;
                return true;
            }
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText(this.descriptionString);
        }
    }

    static class EntitiesLinksMatcher extends TypeSafeMatcher<Iterable<String>> {

        private final Set<String> expectedUris;
        private String mismatchDescription = "Not evaluated";

        EntitiesLinksMatcher(final Set<String> expectedUris) {
            this.expectedUris = expectedUris;
        }

        static EntitiesLinksMatcher matchUrisAnyOrder(
            final String entityApi,
            final String linkedEntityKey,
            final Set<String> optionalHalParams,
            final Iterable<String> entityIds
        ) {
            final Set<String> expectedUris = Sets.newHashSet();
            for (String entityId : entityIds) {
                expectedUris.add(getLinkedResourceExpectedUri(entityApi, entityId, optionalHalParams, linkedEntityKey));
            }
            return new EntitiesLinksMatcher(expectedUris);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean matchesSafely(final Iterable<String> inputUrls) {
            final Set<String> urisToMatch = Sets.newHashSet(this.expectedUris);
            for (String inputUrl : inputUrls) {
                final List<String> matchedUrls = urisToMatch
                    .stream()
                    .filter(inputUrl::endsWith)
                    .collect(Collectors.toList());

                if (matchedUrls.size() == 1) {
                    urisToMatch.remove(matchedUrls.get(0));
                } else if (matchedUrls.size() == 0) {
                    this.mismatchDescription = "Unexpected input URL: " + inputUrl;
                    return false;
                } else {
                    this.mismatchDescription = "Duplicate input URL: " + inputUrl;
                    return false;
                }
            }

            if (!urisToMatch.isEmpty()) {
                this.mismatchDescription = "Unmatched URLs: " + urisToMatch.toString();
                return false;
            }
            this.mismatchDescription = "Successfully matched";
            return true;
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText(this.mismatchDescription);
        }
    }
}
