/*
 *
 *  Copyright 2021 Netflix, Inc.
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
package com.netflix.genie.swagger.spring.autoconfigure;

import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springdoc.core.GroupedOpenApi;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for Swagger via Spring Doc.
 *
 * @author tgianos
 * @see <a href="https://springdoc.org/">Spring Doc</a>
 * @since 4.2.0
 */
@Configuration
public class SwaggerAutoConfiguration {

    /**
     * Bean for the V3 API documentation.
     *
     * @return Instance of {@link GroupedOpenApi} for the V3 endpoints
     */
    @Bean
    @ConditionalOnMissingBean(name = "genieV3ApiGroup")
    public GroupedOpenApi genieV3ApiGroup() {
        return GroupedOpenApi.builder()
            .group("V3")
            .pathsToMatch("/api/v3/**")
            .build();
    }

    /**
     * Configure OpenAPI specification.
     *
     * @return The {@link OpenAPI} instance and description
     */
    @Bean
    @ConditionalOnMissingBean(OpenAPI.class)
    public OpenAPI springShopOpenAPI() {
        return new OpenAPI()
            .info(
                new Info()
                    .title("Genie REST API")
                    .description("Spring shop sample application")
                    .version("v4.2.x")
                    .license(new License().name("Apache 2.0").url("https://www.apache.org/licenses/LICENSE-2.0"))
            )
            .externalDocs(
                new ExternalDocumentation()
                    .description("Documentation Site")
                    .url("https://netflix.github.io/genie")
            );
    }
}
