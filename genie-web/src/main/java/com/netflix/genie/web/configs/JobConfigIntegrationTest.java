package com.netflix.genie.web.configs;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.File;
import java.io.IOException;

/**
 * Configuration overrides for integration tests.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Configuration
@Profile("integration")
public class JobConfigIntegrationTest {

    /**
     * Returns the builds directory of the genie-web probject as the resource.
     *
     * @param resourceLoader The resource loader to use.
     *
     * @return The job dir as a resource.
     *
     * @throws IOException If there is a problem.
     */
    @Bean
    public Resource jobsDir(
        final ResourceLoader resourceLoader
    ) throws IOException {
        final String currentDir = new File(".").getAbsolutePath();
        final Resource jobsDirResource = resourceLoader.getResource(currentDir + "/build/tmp/");
        return jobsDirResource;
    }
}
