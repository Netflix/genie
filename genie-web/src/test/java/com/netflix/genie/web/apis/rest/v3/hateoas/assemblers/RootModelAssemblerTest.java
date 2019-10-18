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
package com.netflix.genie.web.apis.rest.v3.hateoas.assemblers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.hateoas.EntityModel;

/**
 * Unit tests for the RootResourceAssembler.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class RootModelAssemblerTest {

    private RootModelAssembler assembler;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.assembler = new RootModelAssembler();
    }

    /**
     * Make sure we can construct the assembler.
     */
    @Test
    public void canConstruct() {
        Assert.assertNotNull(this.assembler);
    }

    /**
     * Make sure we can convert the DTO to a resource with links.
     */
    @Test
    @Ignore
    public void canConvertToResource() {
        final JsonNode node
            = JsonNodeFactory.instance.objectNode().set("description", JsonNodeFactory.instance.textNode("blah"));
        final EntityModel<JsonNode> model = this.assembler.toModel(node);
        Assert.assertTrue(model.getLinks().hasSize(5));
        Assert.assertNotNull(model.getContent());
        Assert.assertNotNull(model.getLink("self"));
        Assert.assertNotNull(model.getLink("applications"));
        Assert.assertNotNull(model.getLink("commands"));
        Assert.assertNotNull(model.getLink("clusters"));
        Assert.assertNotNull(model.getLink("jobs"));
    }
}
