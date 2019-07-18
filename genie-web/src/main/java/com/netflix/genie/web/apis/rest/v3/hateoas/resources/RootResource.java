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
package com.netflix.genie.web.hateoas.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.hateoas.Resource;

/**
 * Root resource to describe the Genie API.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class RootResource extends Resource<JsonNode> {

    /**
     * Constructor.
     *
     * @param metadata Any json fields that should be added to the root resource.
     */
    @JsonCreator
    public RootResource(final JsonNode metadata) {
        super(metadata);
    }
}
