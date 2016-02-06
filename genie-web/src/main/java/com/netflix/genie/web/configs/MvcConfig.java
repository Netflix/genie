/*
 *
 *  Copyright 2016 Netflix, Inc.
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
import com.netflix.genie.web.resources.handlers.GenieResourceHttpRequestHandler;
import com.netflix.genie.web.resources.writers.DefaultDirectoryWriter;
import com.netflix.genie.web.resources.writers.DirectoryWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.File;
import java.io.IOException;

/**
 * Configuration for Spring MVC.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
public class MvcConfig {

    @Value("${genie.jobs.dir}")
    private String jobsDir;

    @Autowired
    private ApplicationContext context;

    /**
     * Get the directory writer to use.
     *
     * @return A default directory writer
     */
    @Bean
    public DirectoryWriter directoryWriter() {
        return new DefaultDirectoryWriter();
    }

    /**
     * Get a static resource handler for Genie Jobs.
     *
     * @param directoryWriter The directory writer to use for converting directory resources
     * @return The genie resource http request handler.
     * @throws IOException For any issues with files
     */
    @Bean
    public GenieResourceHttpRequestHandler genieResourceHttpRequestHandler(
        final DirectoryWriter directoryWriter
    ) throws IOException {
        final ResourceLoader loader = new DefaultResourceLoader();

        final String slash = "/";
        String localJobsDir = this.jobsDir;
        if (!this.jobsDir.endsWith(slash)) {
            localJobsDir = localJobsDir + slash;
        }
        final Resource jobsDirResource = loader.getResource(localJobsDir);

        if (!jobsDirResource.exists()) {
            final File file = jobsDirResource.getFile();
            if (!file.mkdirs()) {
                throw new IllegalStateException(
                    "Unable to create jobs directory " + this.jobsDir + " and it doesn't exist."
                );
            }
        }

        final GenieResourceHttpRequestHandler handler = new GenieResourceHttpRequestHandler(directoryWriter);
        handler.setApplicationContext(this.context);
        handler.setLocations(Lists.newArrayList(jobsDirResource));

        return handler;
    }
}
