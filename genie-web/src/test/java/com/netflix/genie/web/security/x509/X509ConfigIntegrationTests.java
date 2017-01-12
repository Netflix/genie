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
package com.netflix.genie.web.security.x509;

import com.netflix.genie.GenieWeb;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.web.security.AbstractAPISecurityIntegrationTests;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

/**
 * Integration tests for the X509 based security configuration.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(IntegrationTest.class)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = GenieWeb.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration"})
@TestPropertySource(properties = {"genie.security.x509.enabled = true", "management.security.enabled = true"})
public class X509ConfigIntegrationTests extends AbstractAPISecurityIntegrationTests {

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultMatcher getUnauthorizedExpectedStatus() {
        return MockMvcResultMatchers.status().isForbidden();
    }
}
