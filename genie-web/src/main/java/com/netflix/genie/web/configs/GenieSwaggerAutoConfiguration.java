/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web.configs;

import com.google.common.collect.Lists;
import com.netflix.genie.web.properties.SwaggerProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import springfox.bean.validators.configuration.BeanValidatorPluginsConfiguration;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Spring configuration for Swagger via SpringFox.
 * <p>
 * see: https://github.com/springfox/springfox
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
@ConditionalOnProperty(value = SwaggerProperties.ENABLED_PROPERTY, havingValue = "true")
@EnableSwagger2
@Import(BeanValidatorPluginsConfiguration.class)
@EnableConfigurationProperties(
    {
        SwaggerProperties.class
    }
)
public class GenieSwaggerAutoConfiguration {
    /**
     * Configure Spring Fox.
     *
     * @return The spring fox docket.
     */
    @Bean
    @ConditionalOnMissingBean(name = "genieApi", value = Docket.class)
    public Docket genieApi() {
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
                    Lists.newArrayList()
                )
            )
            .select()
            .apis(RequestHandlerSelectors.basePackage("com.netflix.genie.web.controllers"))
            .paths(PathSelectors.any())
            .build()
            .pathMapping("/")
            .useDefaultResponseMessages(false);
    }

    //TODO: Update with more detailed swagger configurations
    //      see: http://tinyurl.com/glla6vc
}
