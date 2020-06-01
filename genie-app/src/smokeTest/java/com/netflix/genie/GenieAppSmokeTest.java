/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Smoke test to make sure the app comes up successfully with all defaults.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(
    classes = {
        GenieApp.class
    },
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
class GenieAppSmokeTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void testAppStarts() {
        Assertions
            .assertThat(
                this.restTemplate
                    .getForEntity("http://localhost:" + this.port + "/admin/health", String.class)
                    .getStatusCode()
            )
            .isEqualByComparingTo(HttpStatus.OK);
    }

    // TODO: Could add more
}
