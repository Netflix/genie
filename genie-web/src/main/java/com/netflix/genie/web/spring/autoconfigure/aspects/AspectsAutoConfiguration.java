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
package com.netflix.genie.web.spring.autoconfigure.aspects;

import com.netflix.genie.web.aspects.DataServiceRetryAspect;
import com.netflix.genie.web.aspects.HealthCheckMetricsAspect;
import com.netflix.genie.web.aspects.SystemArchitecture;
import com.netflix.genie.web.properties.DataServiceRetryProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * Auto configuration for aspects that should be applied to a running Genie server instance.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        DataServiceRetryProperties.class
    }
)
@EnableAspectJAutoProxy
public class AspectsAutoConfiguration {

    /**
     * An aspect for retrying data layer API calls.
     *
     * @param retryProperties The properties a user can configure for this aspect
     * @return A {@link DataServiceRetryAspect} instance
     */
    @Bean
    @ConditionalOnMissingBean(DataServiceRetryAspect.class)
    public DataServiceRetryAspect getDataServiceRetryAspect(final DataServiceRetryProperties retryProperties) {
        return new DataServiceRetryAspect(retryProperties);
    }

    /**
     * An aspect for collecting metrics for health checks.
     *
     * @param meterRegistry The metrics repository to use
     * @return The instance of {@link HealthCheckMetricsAspect}
     */
    @Bean
    @ConditionalOnMissingBean(HealthCheckMetricsAspect.class)
    public HealthCheckMetricsAspect healthCheckMetricsAspect(final MeterRegistry meterRegistry) {
        return new HealthCheckMetricsAspect(meterRegistry);
    }

    /**
     * A bean that defines pointcuts for various layers of the Genie system.
     *
     * @return A {@link SystemArchitecture} instance
     */
    @Bean
    @ConditionalOnMissingBean(SystemArchitecture.class)
    public SystemArchitecture systemArchitecture() {
        return new SystemArchitecture();
    }
}
