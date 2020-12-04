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
package com.netflix.genie.web.apis.rest.v3.controllers;

import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.RootModelAssembler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * Rest controller for the V3 API root.
 *
 * @author tgianos
 * @since 3.0.0
 */
@RestController
@RequestMapping(value = "/api/v3")
@Slf4j
public class RootRestController {

    private final RootModelAssembler rootModelAssembler;
    private final Map<String, String> metadata;

    /**
     * Constructor.
     *
     * @param rootModelAssembler The assembler to use to construct resources.
     */
    @Autowired
    public RootRestController(final RootModelAssembler rootModelAssembler) {
        this.rootModelAssembler = rootModelAssembler;
        this.metadata = new HashMap<>();
        this.metadata.put("description", "Genie V3 API");
    }

    /**
     * Get a simple HAL+JSON object which represents the various links available in Genie REST API as an entry point.
     *
     * @return the root resource containing various links to the real APIs
     */
    @GetMapping(produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public EntityModel<Map<String, String>> getRoot() {
        return this.rootModelAssembler.toModel(this.metadata);
    }
}
