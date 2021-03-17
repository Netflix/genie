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

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.ArrayList;

/**
 * Spring configuration for Swagger via SpringFox.
 *
 * @author tgianos
 * @see <a href="https://github.com/springfox/springfox">Spring Fox</a>
 * @since 3.0.0
 */
@Configuration
@EnableSwagger2
public class SwaggerAutoConfiguration {

    /**
     * Configure Spring Fox.
     *
     * @return The spring fox docket.
     */
    @Bean
    @ConditionalOnMissingBean(Docket.class)
    public Docket genieApiDocket() {
        return new Docket(DocumentationType.SWAGGER_2)
            .apiInfo(
                new ApiInfo(
                    "Genie REST API",
                    "See our <a href=\"http://netflix.github.io/genie\">GitHub Page</a> for more "
                        + "documentation.<br/>Post any issues found "
                        + "<a href=\"https://github.com/Netflix/genie/issues\">here</a>.<br/>",
                    "4.0.0",
                    null,
                    new Contact("Netflix, Inc.", "https://jobs.netflix.com/", null),
                    "Apache 2.0",
                    "http://www.apache.org/licenses/LICENSE-2.0",
                    new ArrayList<>()
                )
            )
            .select()
            .apis(RequestHandlerSelectors.basePackage("com.netflix.genie.web.apis.rest.v3.controllers"))
            .paths(PathSelectors.any())
            .build()
            .pathMapping("/")
            .useDefaultResponseMessages(false);
    }
}
