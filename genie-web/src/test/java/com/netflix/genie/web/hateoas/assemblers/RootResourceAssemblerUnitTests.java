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
package com.netflix.genie.web.hateoas.assemblers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.hateoas.resources.RootResource;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit tests for the RootResourceAssembler.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class RootResourceAssemblerUnitTests {

    private RootResourceAssembler assembler;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.assembler = new RootResourceAssembler();
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
        final RootResource resource = this.assembler.toResource(node);
        Assert.assertThat(resource.getLinks().size(), Matchers.is(5));
        Assert.assertNotNull(resource.getContent());
        Assert.assertNotNull(resource.getLink("self"));
        Assert.assertNotNull(resource.getLink("applications"));
        Assert.assertNotNull(resource.getLink("commands"));
        Assert.assertNotNull(resource.getLink("clusters"));
        Assert.assertNotNull(resource.getLink("jobs"));
    }
}
