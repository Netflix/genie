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
package com.netflix.genie.web.docs;


import com.netflix.genie.web.GenieWeb;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.restdocs.RestDocumentation;
import org.springframework.restdocs.mockmvc.MockMvcRestDocumentation;
import org.springframework.restdocs.mockmvc.RestDocumentationResultHandler;
import org.springframework.restdocs.operation.preprocess.Preprocessors;
import org.springframework.restdocs.payload.PayloadDocumentation;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.servlet.RequestDispatcher;

/**
 * Used to generate the documentation for the Genie REST APIs.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ActiveProfiles({"docs"})
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = GenieWeb.class)
@WebIntegrationTest(randomPort = true)
public class SpringRestDocs {

    /**
     * Where to put the generated documentation.
     */
    @Rule
    public final RestDocumentation restDocumentation = new RestDocumentation("build/generated-snippets");

    @Autowired
    private WebApplicationContext context;

    private RestDocumentationResultHandler document;
    private MockMvc mockMvc;

    /**
     * Setup the tests.
     */
    @Before
    public void setUp() {
        this.document = MockMvcRestDocumentation.document(
                "{method-name}",
                Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
                Preprocessors.preprocessResponse(Preprocessors.prettyPrint())
        );

        this.mockMvc = MockMvcBuilders
                .webAppContextSetup(this.context)
                .apply(MockMvcRestDocumentation.documentationConfiguration(this.restDocumentation))
                .alwaysDo(this.document)
                .build();
    }

    /**
     * Test to document error responses.
     *
     * @throws Exception For any error
     */
    @Test
    public void errorExample() throws Exception {
        this.document.snippets(
                PayloadDocumentation.responseFields(
                        PayloadDocumentation
                                .fieldWithPath("error")
                                .description("The HTTP error that occurred, e.g. `Bad Request`"),
                        PayloadDocumentation
                                .fieldWithPath("message")
                                .description("A description of the cause of the error"),
                        PayloadDocumentation
                                .fieldWithPath("path")
                                .description("The path to which the request was made"),
                        PayloadDocumentation
                                .fieldWithPath("status")
                                .description("The HTTP status code, e.g. `400`"),
                        PayloadDocumentation
                                .fieldWithPath("timestamp")
                                .description("The time, in milliseconds, at which the error occurred")
                )
        );

        this.mockMvc
                .perform(MockMvcRequestBuilders
                        .get("/error")
                        .requestAttr(RequestDispatcher.ERROR_STATUS_CODE, 400)
                        .requestAttr(RequestDispatcher.ERROR_REQUEST_URI, "/notes")
                        .requestAttr(
                                RequestDispatcher.ERROR_MESSAGE,
                                "The tag 'http://localhost:8080/tags/123' does not exist"
                        )
                )
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isBadRequest())
                .andExpect(MockMvcResultMatchers.jsonPath("error", Matchers.is("Bad Request")))
                .andExpect(MockMvcResultMatchers.jsonPath("timestamp", Matchers.is(Matchers.notNullValue())))
                .andExpect(MockMvcResultMatchers.jsonPath("status", Matchers.is(400)))
                .andExpect(MockMvcResultMatchers.jsonPath("path", Matchers.is(Matchers.notNullValue())));
    }
}
