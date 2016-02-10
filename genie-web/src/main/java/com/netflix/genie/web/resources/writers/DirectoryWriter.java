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
package com.netflix.genie.web.resources.writers;

import org.hibernate.validator.constraints.URL;

import javax.validation.constraints.NotNull;
import java.io.File;

/**
 * Interface for methods to convert a directory to various String representations.
 *
 * @author tgianos
 * @since 3.0.0
 */
public interface DirectoryWriter {

    /**
     * Convert a given directory to an String containing a full valid HTML page.
     *
     * @param directory     The directory to convert. Not null. Is directory.
     * @param requestURL    The URL of the request that kicked off this process
     * @param includeParent Whether the conversion should include reference to the parent directory.
     * @return String HTML representation of the directory
     * @throws Exception for any conversion problem
     */
    String toHtml(
        @NotNull final File directory,
        @URL final String requestURL,
        final boolean includeParent
    ) throws Exception;

    /**
     * Convert a given directory to an String of JSON.
     *
     * @param directory     The directory to convert. Not null. Is directory.
     * @param requestURL    The URL of the request that kicked off this process
     * @param includeParent Whether the conversion should include reference to the parent directory.
     * @return String HTML representation of the directory
     * @throws Exception for any conversion problem
     */
    String toJson(
        @NotNull final File directory,
        @URL final String requestURL,
        final boolean includeParent
    ) throws Exception;
}
