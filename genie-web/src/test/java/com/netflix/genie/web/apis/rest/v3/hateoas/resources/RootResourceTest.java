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
package com.netflix.genie.web.apis.rest.v3.hateoas.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the RootResource class.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class RootResourceTest {

    /**
     * Make sure we can build the resource.
     */
    @Test
    public void canBuildResource() {
        final JsonNode node
            = JsonNodeFactory.instance.objectNode().set("description", JsonNodeFactory.instance.textNode("blah"));
        Assert.assertNotNull(new RootResource(node));
    }
}
