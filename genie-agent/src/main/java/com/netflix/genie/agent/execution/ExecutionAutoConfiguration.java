/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.agent.execution;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * Spring auto configuration for beans required for job execution.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class ExecutionAutoConfiguration {

    /**
     * Provide a lazy execution context bean if one hasn't already been defined.
     *
     * @return An {@link ExecutionContextImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(ExecutionContext.class)
    public ExecutionContext executionContext() {
        return new ExecutionContextImpl();
    }
}
