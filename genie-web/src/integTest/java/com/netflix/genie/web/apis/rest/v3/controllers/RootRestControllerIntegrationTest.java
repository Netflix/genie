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
package com.netflix.genie.web.apis.rest.v3.controllers;

import io.restassured.RestAssured;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.HttpStatus;

/**
 * Integration tests for the Root REST API.
 *
 * @author tgianos
 * @since 3.0.0
 */
class RootRestControllerIntegrationTest extends RestControllerIntegrationTestBase {

    @Test
    void canGetRootResource() {
        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get("/api/v3")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("description", Matchers.notNullValue())
            .body(LINKS_PATH + ".keySet().size()", Matchers.is(5))
            .body(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(APPLICATIONS_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(COMMANDS_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(CLUSTERS_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey(JOBS_LINK_KEY));
    }
}
