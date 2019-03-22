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
package com.netflix.genie.security.saml;

import com.netflix.genie.GenieTestApp;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

/**
 * Integration tests for the SAML security configuration.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Ignore
@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = GenieTestApp.class,
    webEnvironment = SpringBootTest.WebEnvironment.MOCK
)
@ActiveProfiles({"saml", "integration"})
@AutoConfigureMockMvc
public class SAMLConfigIntegrationTest {

    @Autowired
    private MockMvc mvc;

    /**
     * Make sure we can get root.
     *
     * @throws Exception on any error
     */
    @Test
    public void canGetRoot() throws Exception {
        final ResultActions resultActions = this.mvc.perform(MockMvcRequestBuilders.get("/"));

        resultActions.andExpect(MockMvcResultMatchers.status().isOk());
    }
}
