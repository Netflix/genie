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
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.util.GenieObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities to convert arguments during parsing.
 *
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

            if (StringUtils.isBlank(value)) {
                throw new ParameterException("Invalid file: '" + value + "'");
            }
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

    static final class CriterionConverter implements IStringConverter<Criterion> {
        // Used to guarantee consistency in all places used (documentation, multiple regex)
        private static final String ID_KEY = "ID";
        private static final String NAME_KEY = "NAME";
        private static final String VERSION_KEY = "VERSION";
        private static final String STATUS_KEY = "STATUS";
        private static final String TAGS_KEY = "TAGS";

        // Used to guarantee consistency between regex compilation and matcher access
        private static final String ID_CAPTURE_GROUP = "id";
        private static final String NAME_CAPTURE_GROUP = "name";
        private static final String VERSION_CAPTURE_GROUP = "version";
        private static final String STATUS_CAPTURE_GROUP = "status";
        private static final String TAGS_CAPTURE_GROUP = "tags";

        private static final String EXAMPLE_CRITERION
            = ID_KEY + "=i/" + NAME_KEY + "=n/" + VERSION_KEY + "=v/" + STATUS_KEY + "=s/" + TAGS_KEY + "=t1,t2,t3\n";

        private static final String COMPONENT_STRING = StringUtils.join(
            Lists.newArrayList(
                ID_KEY,
                NAME_KEY,
                VERSION_KEY,
                STATUS_KEY,
                TAGS_KEY
            ),
            ','
        );

        static final String CRITERION_SYNTAX_MESSAGE = "CRITERION SYNTAX:\n"
            + "Criterion is parsed as a string in the format:\n"
            + "    " + EXAMPLE_CRITERION
            + "Note:\n"
            + " - All components (" + COMPONENT_STRING + ") are optional, but at least one is required\n"
            + " - Order of components is enforced (i.e. NAME cannot appear before ID)\n"
            + " - Values cannot be empty (skip the components altogether if no value is present)\n";

        private static final Pattern SINGLE_CRITERION_PATTERN = Pattern.compile(
            "^" + ID_KEY + "=(?<" + ID_CAPTURE_GROUP + ">[^/]+)|"
                + NAME_KEY + "=(?<" + NAME_CAPTURE_GROUP + ">[^/]+)|"
                + VERSION_KEY + "=(?<" + VERSION_CAPTURE_GROUP + ">[^/]+)|"
                + STATUS_KEY + "=(?<" + STATUS_CAPTURE_GROUP + ">[^/]+)|"
                + TAGS_KEY + "=(?<" + TAGS_CAPTURE_GROUP + ">[^/]+)$"
        );

        private static final Pattern MULTI_CRITERION_PATTERN = Pattern.compile(
            "^(" + ID_KEY + "=(?<" + ID_CAPTURE_GROUP + ">[^/]+)/)?"
                + "(" + NAME_KEY + "=(?<" + NAME_CAPTURE_GROUP + ">[^/]+))?/?"
                + "(" + VERSION_KEY + "=(?<" + VERSION_CAPTURE_GROUP + ">[^/]+))?/?"
                + "(" + STATUS_KEY + "=(?<" + STATUS_CAPTURE_GROUP + ">[^/]+))?/?"
                + "(" + TAGS_KEY + "=(?<" + TAGS_CAPTURE_GROUP + ">[^/]+))?$"
        );

        @Override
        public Criterion convert(final String value) {

            final Criterion.Builder criterionBuilder = new Criterion.Builder();

            final Matcher multiComponentMatcher = MULTI_CRITERION_PATTERN.matcher(value);
            final Matcher singleComponentMatcher = SINGLE_CRITERION_PATTERN.matcher(value);

            final Matcher matchingMatcher;

            if (multiComponentMatcher.matches()) {
                matchingMatcher = multiComponentMatcher;
            } else if (singleComponentMatcher.matches()) {
                matchingMatcher = singleComponentMatcher;
            } else {
                throw new ParameterException("Invalid criterion: " + value);
            }

            final String id = matchingMatcher.group(ID_CAPTURE_GROUP);
            final String name = matchingMatcher.group(NAME_CAPTURE_GROUP);
            final String version = matchingMatcher.group(VERSION_CAPTURE_GROUP);
            final String status = matchingMatcher.group(STATUS_CAPTURE_GROUP);
            final String tags = matchingMatcher.group(TAGS_CAPTURE_GROUP);
            final Set<String> splitTags = tags == null ? null : Sets.newHashSet(tags.split(","));

            criterionBuilder
                .withId(id)
                .withName(name)
                .withVersion(version)
                .withStatus(status)
                .withTags(splitTags);

            try {
                return criterionBuilder.build();
            } catch (final IllegalArgumentException e) {
                throw new ParameterException("Invalid criterion: " + value, e);
            }
        }
    }

    static final class JSONConverter implements IStringConverter<JsonNode> {
        @Override
        public JsonNode convert(final String value) {
            try {
                return GenieObjectMapper.getMapper().readTree(value);
            } catch (final IOException e) {
                throw new ParameterException("Failed to parse JSON argument", e);
            }
        }
    }

    static final class UriOrLocalPathConverter implements IStringConverter<String> {
        static final String ATTACHMENT_HELP_MESSAGE = "ATTACHMENTS:\n"
            + "Different kinds of job-specific files can be attached to a job.\n"
            + "These are broken down by Genie in 3 categories:\n"
            + " - Configurations: to configure components or tools (e.g. properties files, XML, YAML. ...)\n"
            + " - Dependencies: binaries or archives (e.g., jar, tar.gz, ...)\n"
            + " - Setup: a shell script sourced before executing the job (to set environment, expand archives, ...)\n"
            + "\n"
            + "These job attachments are downloaded to the job folder during setup and they are archived after \n"
            + "execution (conditional on cleanup and archival options).\n"
            + "\n"
            + "Attachments can either be valid URIs or paths to local files, example:\n"
            + "  s3://configurations/hadoop/spark/1.6.1/hive-site.xml\n"
            + "  http://some-domain.org/some-project/my-config.properties\n"
            + "  file:///tmp/myscript.presto\n"
            + "  file:/tmp/query.sql\n"
            + "  ./myscript.sql (shortcut for file:/${PWD}/myscript.sql)\n";

        /**
         * {@inheritDoc}
         */
        @Override
        public String convert(final String value) {
            final URI uri;
            try {
                uri = new URI(value);
            } catch (URISyntaxException e) {
                throw new ParameterException("Resource URI or path: '" + value + "'", e);
            }

            // If URI has a scheme, leave it alone and pass it along.
            if (uri.getScheme() != null) {
                return uri.toASCIIString();
            }

            // Otherwise try to resolve it as an local file path.
            final URI newUri;
            try {
                newUri = new URI(
                    "file",
                    uri.getHost(),
                    Paths.get(value).toAbsolutePath().normalize().toString(),
                    uri.getQuery(),
                    uri.getFragment()
                );
            } catch (URISyntaxException e) {
                throw new ParameterException("Failed to construct uri for local resource: '" + value + "'", e);
            }

            return newUri.toASCIIString();
        }
    }
}
