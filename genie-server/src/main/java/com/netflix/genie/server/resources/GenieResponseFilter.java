/*
 *
 *  Copyright 2014 Netflix, Inc.
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

import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.ext.Provider;

/**
 * Used to increment various counters based on response from Jersey.
 *
 * @author tgianos
 */
@Provider
@Named
public class GenieResponseFilter implements ContainerResponseFilter {

    private GenieNodeStatistics statistics;

    /**
     * Constructor.
     *
     * @param statistics The genie node statistics to use to capture information.
     */
    @Inject
    public GenieResponseFilter(final GenieNodeStatistics statistics) {
        this.statistics = statistics;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContainerResponse filter(final ContainerRequest request, final ContainerResponse response) {
        final int status = response.getStatus();
        if (status >= 200 && status < 300) {
            this.statistics.incrGenie2xxCount();
        } else if (status >= 400 && status < 500) {
            this.statistics.incrGenie4xxCount();
        } else if (status >= 500) {
            this.statistics.incrGenie5xxCount();
        }
        return response;
    }
}
