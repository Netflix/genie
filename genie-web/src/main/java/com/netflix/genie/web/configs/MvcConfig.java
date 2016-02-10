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
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

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
     * Get the hostname for this application. This is the default fallback implementation if no other bean with
     * id hostname has been created by another profile.
     *
     * @return The hostname calculated from InetAddress
     * @throws UnknownHostException When the host can't be calculated
     * @see InetAddress#getCanonicalHostName()
     */
    @Bean
    @ConditionalOnMissingBean
    public String hostname() throws UnknownHostException {
        return "a";
//        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    /**
     * Get an HttpClient for calling between Genie nodes.
     *
     * @return The http client to use
     */
    @Bean
    @ConditionalOnMissingBean
    public HttpClient genieMvcHttpClient() {
        return HttpClients.createDefault();
    }

    /**
     * Get the directory writer to use.
     *
     * @return A default directory writer
     */
    @Bean
    @ConditionalOnMissingBean
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
    @ConditionalOnMissingBean
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
