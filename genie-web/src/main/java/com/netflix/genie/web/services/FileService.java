/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.services;

import com.netflix.genie.common.exceptions.GenieException;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.validation.annotation.Validated;

/**
 * API definition for manipulating file references within Genie.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Validated
public interface FileService {

    /**
     * Attempt to create a file reference in the system if it doesn't already exist.
     *
     * @param file the file to create. Not blank.
     * @throws GenieException on any error except that the file already exists
     */
    void createFileIfNotExists(@NotBlank final String file) throws GenieException;
}
