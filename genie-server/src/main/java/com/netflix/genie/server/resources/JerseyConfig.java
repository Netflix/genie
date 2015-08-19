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
package com.netflix.genie.server.resources;

import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.beans.factory.annotation.Value;

import javax.inject.Named;
import javax.ws.rs.ApplicationPath;

/**
 * Configure Jersey services.
 *
 * @author tgianos
 */
@Named
@ApplicationPath("genie")
public class JerseyConfig extends ResourceConfig {

    @Value("${com.netflix.genie.version}")
    private String version;

    /**
     * Constructor which sets up the various bindings.
     */
    public JerseyConfig() {
        setApplicationName("Genie");

        // Genie Resources
        register(ApplicationConfigResource.class);
        register(ClusterConfigResource.class);
        register(CommandConfigResource.class);
        register(ConstraintViolationExceptionMapper.class);
        register(GenieExceptionMapper.class);
        register(GenieResponseFilter.class);
        register(JobResource.class);

        // Swagger
        register(ApiListingResource.class);
        register(SwaggerSerializers.class);

        // Configure Swagger
        final BeanConfig beanConfig = new BeanConfig();
        beanConfig.setTitle("Genie REST API");
        beanConfig.setDescription(
                "See our <a href=\"http://netflix.github.io/genie\">"
                        + "GitHub Page</a> for more documentation.<br/>Post any issues found "
                        + "<a href=\"https://github.com/Netflix/genie/issues\">here</a>.<br/>"
        );
        beanConfig.setContact("Netflix, Inc.");
        beanConfig.setLicense("Apache 2.0");
        beanConfig.setLicenseUrl("http://www.apache.org/licenses/LICENSE-2.0");
        beanConfig.setVersion(this.version);
        beanConfig.setSchemes(new String[]{"http"});
        beanConfig.setHost("localhost:8080");
        beanConfig.setBasePath("/genie");
        beanConfig.setResourcePackage("com.netflix.genie.server.resources");
        beanConfig.setScan(true);
        beanConfig.setPrettyPrint(true);
    }
}
