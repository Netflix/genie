package com.netflix.genie.web.controllers;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;
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
@Slf4j
public class UIControllerIntegrationTest extends RestControllerIntegrationTestsBase {

    /**
     * Test forwarding of various UI pages to index.html.
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
                .andExpect(MockMvcResultMatchers.forwardedUrl("index.html"))
                .andReturn();
        }
    }

    /**
     * Test serving of the index.html file.
     * @throws Exception in case of error
     */
    @Test
    public void testGetIndex() throws Exception {

        final byte[] indexResourceBytes;
        InputStream is = null;

        try {
            is = UIController.class.getResourceAsStream("/static/index.html");
            Assert.assertNotNull(is);
            Assert.assertTrue(is.available() > 0);
            indexResourceBytes = IOUtils.toByteArray(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }

        this.mvc
            .perform(MockMvcRequestBuilders.get("/index.html"))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentTypeCompatibleWith(MediaType.TEXT_HTML))
            .andExpect(MockMvcResultMatchers.content().encoding(StandardCharsets.UTF_8.name()))
            .andExpect(MockMvcResultMatchers.content().bytes(indexResourceBytes))
            .andReturn();
    }

    /**
     * Test forwarding of job files to the jobs API.
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
