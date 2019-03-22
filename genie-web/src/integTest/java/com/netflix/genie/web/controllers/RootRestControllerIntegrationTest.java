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

import io.restassured.RestAssured;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.HttpStatus;

/**
 * Integration tests for the Root REST API.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class RootRestControllerIntegrationTest extends RestControllerIntegrationTestBase {

    /**
     * {@inheritDoc}
     */
    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
    }

    /**
     * {@inheritDoc}
     */
    @After
    @Override
    public void cleanup() throws Exception {
        super.cleanup();
    }

    /**
     * Make sure we can get the root resource.
     */
    @Test
    public void canGetRootResource() {
        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get("/api/v3")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body("content.description", Matchers.notNullValue())
            .body(LINKS_PATH + ".keySet().size()", Matchers.is(5))
            .body(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(APPLICATIONS_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(CLUSTERS_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(JOBS_LINK_KEY));
    }
}
