/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.ui.controllers;

import com.netflix.genie.web.apis.rest.v3.controllers.GenieExceptionMapper;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Integration tests for {@link UIController}.
 *
 * @author mprimi
 * @since 3.2.0
 */
@RunWith(SpringRunner.class)
@WebMvcTest(UIController.class)
@ActiveProfiles("integration")
public class UIControllerIntegrationTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private GenieExceptionMapper genieExceptionMapper;

    /**
     * Test forwarding of various UI pages to index.html.
     *
     * @throws Exception in case of error
     */
    @Test
    public void testForwardingToIndex() throws Exception {
        final List<String> validPaths = Arrays.asList(
            "/",
            "/applications",
            "/clusters",
            "/commands",
            "/jobs",
            "/output"
        );

        for (String validPath : validPaths) {
            this.mvc
                .perform(MockMvcRequestBuilders.get(validPath))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.view().name("index"))
                .andReturn();
        }
    }

    /**
     * Test serving of the index.html file.
     *
     * @throws Exception in case of error
     */
    @Test
    public void testGetIndex() throws Exception {
        final String indexContent;

        try (InputStream is = UIController.class.getResourceAsStream("/templates/index.html")) {
            Assert.assertNotNull(is);
            Assert.assertTrue(is.available() > 0);
            indexContent = IOUtils.toString(is, StandardCharsets.UTF_8);
        }

        final List<String> validPaths = Arrays.asList(
            "/",
            "/applications",
            "/clusters",
            "/commands",
            "/jobs",
            "/output/12345"
        );

        for (String validPath : validPaths) {
            this.mvc
                .perform(MockMvcRequestBuilders.get(validPath))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaType.TEXT_HTML))
                .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
                .andExpect(MockMvcResultMatchers.content().string(indexContent))
                .andReturn();
        }
    }

    /**
     * Test forwarding of job files to the jobs API.
     *
     * @throws Exception in case of error
     */
    @Test
    public void getFile() throws Exception {
        final String jobId = UUID.randomUUID().toString();
        final String file = "foo/bar.txt";

        this.mvc
            .perform(MockMvcRequestBuilders.get("/file/{id}/{path}", jobId, file))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.forwardedUrl("/api/v3/jobs/" + jobId + "/" + file))
            .andReturn();
    }
}
