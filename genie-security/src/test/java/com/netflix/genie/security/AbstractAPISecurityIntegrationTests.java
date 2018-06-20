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
package com.netflix.genie.security;

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.UUID;

/**
 * Shared tests for accessing API resources. Any API configuration integration tests should extend this for consistent
 * behavior.
 *
 * @author tgianos
 * @since 3.0.0
 */
public abstract class AbstractAPISecurityIntegrationTests {

    private static final Application APPLICATION =
        new Application.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            ApplicationStatus.ACTIVE
        )
            .withId(UUID.randomUUID().toString())
            .build();

    private static final Cluster CLUSTER =
        new Cluster.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            ClusterStatus.UP
        )
            .withId(UUID.randomUUID().toString())
            .build();

    private static final Command COMMAND =
        new Command.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            CommandStatus.ACTIVE,
            UUID.randomUUID().toString(),
            1000L
        )
            .withId(UUID.randomUUID().toString())
            .build();

    private static final String APPLICATIONS_API = "/api/v3/applications";
    private static final String CLUSTERS_API = "/api/v3/clusters";
    private static final String COMMANDS_API = "/api/v3/commands";
    private static final String JOBS_API = "/api/v3/jobs";
    private static final ResultMatcher OK = MockMvcResultMatchers.status().isOk();
    private static final ResultMatcher BAD_REQUEST = MockMvcResultMatchers.status().isBadRequest();
    private static final ResultMatcher CREATED = MockMvcResultMatchers.status().isCreated();
    private static final ResultMatcher NO_CONTENT = MockMvcResultMatchers.status().isNoContent();
    private static final ResultMatcher NOT_FOUND = MockMvcResultMatchers.status().isNotFound();
    private static final ResultMatcher FORBIDDEN = MockMvcResultMatchers.status().isForbidden();
    private static final ResultMatcher UNAUTHORIZED = MockMvcResultMatchers.status().isUnauthorized();

    @Autowired
    private WebEndpointProperties endpointProperties;

    @Autowired
    private WebApplicationContext context;

    @Autowired
    private JpaApplicationRepository applicationRepository;

    @Autowired
    private JpaClusterRepository clusterRepository;

    @Autowired
    private JpaCommandRepository commandRepository;

    @Autowired
    private JpaJobRepository jobRepository;

    @Autowired
    private JpaFileRepository fileRepository;

    @Autowired
    private JpaTagRepository tagRepository;

    private MockMvc mvc;

    /**
     * What ResultMatcher this class should return for unauthorized calls. For example x509 returns 403 while OAuth2
     * returns 401 when a call is made while unauthenticated.
     *
     * @return The result matcher to use when a user isn't logged in
     */
    public abstract ResultMatcher getUnauthorizedExpectedStatus();

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobRepository.deleteAll();
        this.clusterRepository.deleteAll();
        this.commandRepository.deleteAll();
        this.applicationRepository.deleteAll();
        this.fileRepository.deleteAll();
        this.tagRepository.deleteAll();

        this.mvc = MockMvcBuilders
            .webAppContextSetup(this.context)
            .apply(SecurityMockMvcConfigurers.springSecurity())
            .build();
    }

    /**
     * Clean out the db after every test.
     */
    @After
    public void cleanup() {
        this.jobRepository.deleteAll();
        this.clusterRepository.deleteAll();
        this.commandRepository.deleteAll();
        this.applicationRepository.deleteAll();
        this.fileRepository.deleteAll();
        this.tagRepository.deleteAll();
    }

    /**
     * Make sure we can get root.
     *
     * @throws Exception on any error
     */
    @Test
    public void canGetRoot() throws Exception {
        this.mvc.perform(MockMvcRequestBuilders.get("/")).andExpect(MockMvcResultMatchers.status().isOk());
    }

    /**
     * Make sure we can't call any API if not authenticated.
     *
     * @throws Exception on any error
     */
    @Test
    public void cantCallAnyAPIIfUnauthenticated() throws Exception {
        final ResultMatcher expectedUnauthenticatedStatus = this.getUnauthorizedExpectedStatus();
        this.get(APPLICATIONS_API, expectedUnauthenticatedStatus);
        this.get(CLUSTERS_API, expectedUnauthenticatedStatus);
        this.get(COMMANDS_API, expectedUnauthenticatedStatus);
        this.get(JOBS_API, expectedUnauthenticatedStatus);
        this.checkActuatorEndpoints(UNAUTHORIZED);
    }

    /**
     * Make sure we can't call anything under admin control as a regular user.
     *
     * @throws Exception on any error
     */
    @Test
    @WithMockUser
    public void cantCallAdminAPIsAsRegularUser() throws Exception {
        this.get(APPLICATIONS_API, OK);
        this.delete(APPLICATIONS_API, FORBIDDEN);
        this.post(APPLICATIONS_API, APPLICATION, FORBIDDEN);
        this.get(APPLICATIONS_API + "/" + UUID.randomUUID().toString(), NOT_FOUND);
        this.put(APPLICATIONS_API + "/" + UUID.randomUUID().toString(), APPLICATION, FORBIDDEN);

        this.get(CLUSTERS_API, OK);
        this.delete(CLUSTERS_API, FORBIDDEN);
        this.post(CLUSTERS_API, CLUSTER, FORBIDDEN);
        this.get(CLUSTERS_API + "/" + UUID.randomUUID().toString(), NOT_FOUND);
        this.put(CLUSTERS_API + "/" + UUID.randomUUID().toString(), CLUSTER, FORBIDDEN);

        this.get(COMMANDS_API, OK);
        this.delete(COMMANDS_API, FORBIDDEN);
        this.post(COMMANDS_API, COMMAND, FORBIDDEN);
        this.get(COMMANDS_API + "/" + UUID.randomUUID().toString(), NOT_FOUND);
        this.put(COMMANDS_API + "/" + UUID.randomUUID().toString(), COMMAND, FORBIDDEN);

        this.get(JOBS_API, OK);
        this.post(JOBS_API, "{\"key\":\"value\"}", BAD_REQUEST);
        this.get(JOBS_API + "/" + UUID.randomUUID().toString(), NOT_FOUND);
        this.delete(JOBS_API + "/" + UUID.randomUUID().toString(), NOT_FOUND);

        this.checkActuatorEndpoints(FORBIDDEN);
    }

    /**
     * Make sure we get get anything under admin control if we're an admin.
     *
     * @throws Exception on any error
     */
    @Test
    @WithMockUser(roles = {"USER", "ADMIN"})
    public void canCallAdminAPIsAsAdminUser() throws Exception {
        this.get(APPLICATIONS_API, OK);
        this.delete(APPLICATIONS_API, NO_CONTENT);
        this.post(APPLICATIONS_API, APPLICATION, CREATED);
        this.get(APPLICATIONS_API + "/" + UUID.randomUUID().toString(), NOT_FOUND);
        this.put(APPLICATIONS_API + "/" + UUID.randomUUID().toString(), APPLICATION, NOT_FOUND);

        this.get(CLUSTERS_API, OK);
        this.delete(CLUSTERS_API, NO_CONTENT);
        this.post(CLUSTERS_API, CLUSTER, CREATED);
        this.get(CLUSTERS_API + "/" + UUID.randomUUID().toString(), NOT_FOUND);
        this.put(CLUSTERS_API + "/" + UUID.randomUUID().toString(), CLUSTER, NOT_FOUND);

        this.get(COMMANDS_API, OK);
        this.delete(COMMANDS_API, NO_CONTENT);
        this.post(COMMANDS_API, COMMAND, CREATED);
        this.get(COMMANDS_API + "/" + UUID.randomUUID().toString(), NOT_FOUND);
        this.put(COMMANDS_API + "/" + UUID.randomUUID().toString(), COMMAND, NOT_FOUND);

        this.get(JOBS_API, OK);
        this.post(JOBS_API, "{\"key\":\"value\"}", BAD_REQUEST);
        this.get(JOBS_API + "/" + UUID.randomUUID().toString(), NOT_FOUND);
        this.delete(JOBS_API + "/" + UUID.randomUUID().toString(), NOT_FOUND);

        this.checkActuatorEndpoints(OK);
    }

    private void post(final String endpoint, final Object body, final ResultMatcher expectedStatus) throws Exception {
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(endpoint)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(GenieObjectMapper.getMapper().writeValueAsBytes(body))
            ).andExpect(expectedStatus);
    }

    private void put(final String endpoint, final Object body, final ResultMatcher expectedStatus) throws Exception {
        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .put(endpoint)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(GenieObjectMapper.getMapper().writeValueAsBytes(body))
            ).andExpect(expectedStatus);
    }

    private void get(final String endpoint, final ResultMatcher expectedStatus) throws Exception {
        this.mvc.perform(MockMvcRequestBuilders.get(endpoint)).andExpect(expectedStatus);
    }

    private void delete(final String endpoint, final ResultMatcher expectedStatus) throws Exception {
        this.mvc.perform(MockMvcRequestBuilders.delete(endpoint)).andExpect(expectedStatus);
    }

    private void checkActuatorEndpoints(final ResultMatcher expectedResult) throws Exception {
        // See: https://docs.spring.io/spring-boot/docs/current/reference/html/production-ready-endpoints.html
//        this.get(this.endpointProperties.getBasePath() + "/autoconfig", expectedResult);
//        this.get(this.endpointProperties.getBasePath() + "/auditevents", expectedResult);
//        this.get(this.endpointProperties.getBasePath() + "/beans", expectedResult);
//        this.get(this.endpointProperties.getBasePath() + "/configprops", expectedResult);
//        this.get(this.endpointProperties.getBasePath() + "/dump", expectedResult);
//        this.get(this.endpointProperties.getBasePath() + "/env", expectedResult);
        this.get(this.endpointProperties.getBasePath() + "/health", OK);
        this.get(this.endpointProperties.getBasePath() + "/info", OK);
//        this.get(this.endpointProperties.getBasePath() + "/loggers", expectedResult);
//        this.get(this.endpointProperties.getBasePath() + "/mappings", expectedResult);
//        this.get(this.endpointProperties.getBasePath() + "/metrics", expectedResult);
//        this.get(this.endpointProperties.getBasePath() + "/trace", expectedResult);
    }
}
