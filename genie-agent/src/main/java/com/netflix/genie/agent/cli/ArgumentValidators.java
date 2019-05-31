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

import com.amazonaws.services.s3.AmazonS3URI;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.validators.PositiveInteger;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Parameter validators for command-line arguments.
 *
 * @author mprimi
 * @since 4.0.0
 */
final class ArgumentValidators {

    /**
     * Hide constructor.
     */
    private ArgumentValidators() {
    }

    /**
     * Validates a string parameter is not null or empty.
     */
    public static class StringValidator implements IParameterValidator {

        /**
         * {@inheritDoc}
         */
        @Override
        public void validate(final String name, final String value) throws ParameterException {
            if (StringUtils.isBlank(value)) {
                throw new ParameterException(name + " is null or empty");
            }
        }
    }

    /**
     * Validates a string parameter is a valid S3 uri.
     */
    public static class S3URIValidator implements IParameterValidator {

        /**
         * {@inheritDoc}
         */
        @Override
        public void validate(final String name, final String value) throws ParameterException {
            try {
                //Check if a valid S3 uri can be created
                new AmazonS3URI(value);
            } catch (Exception e) {
                throw new ParameterException(name + " is not a valid S3 uri");
            }
        }
    }

    /**
     * Validates a integer parameter is a positive integer.
     */
    public static class PortValidator extends PositiveInteger {
    }

    /**
     * Validates an URI parameter can be parsed as URI.
     * If the resource type is local file, validate it exists, it is readable, etc.
     */
    public static class URIValidator implements IValueValidator<String> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void validate(final String name, final String value) throws ParameterException {
            // Make sure the value is a valid URI
            final URI uri;
            try {
                uri = new URI(value);
            } catch (URISyntaxException e) {
                throw new ParameterException("Invalid URI " + value + " (for option: " + name + ")", e);
            }

            // If it's a file, make sure it exists, it's a file, it's readable, etc.
            if (uri.getScheme().equals("file")) {
                final Path path = Paths.get(uri.getPath());

                if (!Files.exists(path)) {
                    throw new ParameterException("File " + value + " does not exist (for option: " + name + ")");
                } else if (!Files.isRegularFile(path)) {
                    throw new ParameterException("File " + value + " is not a file (for option: " + name + ")");
                } else if (!Files.isReadable(path)) {
                    throw new ParameterException("File " + value + " is not readable (for option: " + name + ")");
                }
            }
        }
    }

    /**
     * Validates a URI collection parameter by delegating validation to {@link URIValidator}.
     */
    public static class URIListValidator implements IValueValidator<List<String>> {

        /**
         * {@inheritDoc}
         */
        @Override
        public void validate(final String name, final List<String> values) throws ParameterException {
            final URIValidator validator = new URIValidator();
            for (final String value : values) {
                validator.validate(name, value);
            }
        }
    }
}
