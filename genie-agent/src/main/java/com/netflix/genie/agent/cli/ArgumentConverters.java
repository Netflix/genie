/*
 *
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.genie.agent.cli;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utilities to convert arguments during parsing.
 * @author mprimi
 * @since 4.0.0
 */
final class ArgumentConverters {

    /**
     * Hide constructor.
     */
    private ArgumentConverters() {
    }

    static final class FileConverter implements IStringConverter<File> {
        @Override
        public File convert(final String value) {
            return new File(value);
        }
    }

    static final class URIConverter implements IStringConverter<URI> {
        @Override
        public URI convert(final String value) {
            try {
                return new URI(value);
            } catch (final URISyntaxException e) {
                throw new ParameterException("Invalid URI: " + value, e);
            }
        }
    }
}
