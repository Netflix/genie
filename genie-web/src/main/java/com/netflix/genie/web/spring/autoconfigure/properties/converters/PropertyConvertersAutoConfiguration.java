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
package com.netflix.genie.web.spring.autoconfigure.properties.converters;

import com.netflix.genie.web.properties.converters.URIPropertyConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto configuration for registering {@link org.springframework.boot.context.properties.ConfigurationPropertiesBinding}
 * beans.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class PropertyConvertersAutoConfiguration {

    /**
     * Provide a converter which will convert Strings to absolute {@link java.net.URI} instances.
     *
     * @return A {@link URIPropertyConverter} instance.
     */
    @Bean
    public URIPropertyConverter uriPropertyConverter() {
        return new URIPropertyConverter();
    }
}
