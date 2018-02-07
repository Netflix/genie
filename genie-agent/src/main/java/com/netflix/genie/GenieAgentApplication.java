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
package com.netflix.genie;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;

/**
 * Genie Agent application.
 *
 * @author mprimi
 * @since 4.0.0
 */
@SpringBootApplication(
    exclude = {
        JmxAutoConfiguration.class,
    }
)
public class GenieAgentApplication {
    /**
     * Main method, actual execution is delegated to GenieAgentRunnner.
     *
     * @param args command-line arguments
     */
    public static void main(final String[] args) {
        System.exit(new GenieAgentApplication().run(args));
    }

    private int run(final String[] args) {
        return SpringApplication.exit(
            SpringApplication.run(
                GenieAgentApplication.class, args
            )
        );
    }
}
