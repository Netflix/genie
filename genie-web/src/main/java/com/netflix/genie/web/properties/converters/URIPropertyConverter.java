/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.properties.converters;

import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.core.convert.converter.Converter;

import java.net.URI;

/**
 * A converter between a String and a {@link URI} to enforce well formatted schema representations of resources.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ConfigurationPropertiesBinding
public class URIPropertyConverter implements Converter<String, URI> {

    /**
     * {@inheritDoc}
     */
    @Override
    public URI convert(final String source) {
        final URI uri = URI.create(source);

        // Make sure we have an absolute URI
        if (!uri.isAbsolute()) {
            throw new IllegalArgumentException("The supplied URI (" + source + ") is not absolute");
        }

        return uri;
    }
}
