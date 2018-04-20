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

/**
 * Genie Agent application.
 *
 * @author mprimi
 * @since 4.0.0
 */
@SpringBootApplication
public class GenieAgentApplication {
    /**
     * Main method, actual execution is delegated to GenieAgentRunner.
     *
     * @param args command-line arguments
     */
    public static void main(final String[] args) {
        System.exit(new GenieAgentApplication().run(args));
    }

    private int run(final String[] args) {
        final SpringApplication app = new SpringApplication(GenieAgentApplication.class);
        // Disable parsing of command-line arguments into properties.
        app.setAddCommandLineProperties(false);
        return SpringApplication.exit(app.run(args));
    }
}
