package com.netflix.genie.web.controllers;

import com.netflix.genie.GenieWeb;
import com.netflix.genie.test.categories.IntegrationTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
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
 * Integration tests for UI Controller.
 *
 * @author mprimi
 * @since 3.2.0
 */
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(classes = GenieWeb.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(resolver = IntegrationTestActiveProfilesResolver.class)
@AutoConfigureMockMvc
@Slf4j
public class UIControllerIntegrationTest {

    @Autowired
    private MockMvc mvc;

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
        InputStream is = null;

        try {
            is = UIController.class.getResourceAsStream("/templates/index.html");
            Assert.assertNotNull(is);
            Assert.assertTrue(is.available() > 0);
            indexContent = IOUtils.toString(is, StandardCharsets.UTF_8);
        } finally {
            if (is != null) {
                is.close();
            }
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
