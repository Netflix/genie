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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Smoke test to make sure the app comes up successfully with all defaults.
 *
 * @author tgianos
 * @since 4.0.0
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = {
        GenieWar.class
    },
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
public class GenieWarSmokeTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    /**
     * Make sure the app will start correctly and the health check returns a 200.
     */
    @Test
    public void testAppStarts() {
        Assertions.assertThat(
            this.restTemplate
                .getForEntity("http://localhost:" + this.port + "/admin/health", String.class)
                .getStatusCode()
        ).isEqualByComparingTo(HttpStatus.OK);
    }

    // TODO: Could add more
}
