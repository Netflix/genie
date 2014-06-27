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
package com.netflix.genie.server.startup;

import com.google.inject.servlet.ServletModule;
import com.google.inject.spring.SpringIntegration;
import com.netflix.genie.server.jobmanager.JobJanitor;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.metrics.JobCountManager;
import com.netflix.genie.server.metrics.JobCountMonitor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * A Guice module which binds spring beans to the Guice binder for integration.
 *
 * @author tgianos
 */
public class GenieModule extends ServletModule {

    /**
     * Configure the Guice bindings.
     */
    @Override
    protected void configureServlets() {
        final ApplicationContext springApplicationContext
                = WebApplicationContextUtils.getWebApplicationContext(getServletContext());
        bind(BeanFactory.class).toInstance(springApplicationContext);
        bind(JobJanitor.class).toProvider(
                SpringIntegration.fromSpring(
                        JobJanitor.class,
                        "JobJanitorImpl")
        );
        bind(GenieNodeStatistics.class).toProvider(
                SpringIntegration.fromSpring(
                        GenieNodeStatistics.class,
                        "GenieNodeStatisticsImpl")
        );
        bind(JobCountManager.class).toProvider(
                SpringIntegration.fromSpring(
                        JobCountManager.class,
                        "JobCountManagerImpl")
        );
        bind(JobCountMonitor.class).toProvider(
                SpringIntegration.fromSpring(
                        JobCountMonitor.class,
                        "JobCountMonitorImpl")
        );
    }
}
