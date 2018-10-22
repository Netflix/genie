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

import com.netflix.genie.agent.cli.UserConsole;
import com.netflix.genie.agent.cli.Util;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.dao.PersistenceExceptionTranslationAutoConfiguration;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.context.annotation.Configuration;

/**
 * Genie Agent application.
 *
 * @author mprimi
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@EnableAutoConfiguration(
    exclude = {
        /*
         * Picked up by default but not believed to be needed currently
         */
        GsonAutoConfiguration.class,
        JacksonAutoConfiguration.class,
        PersistenceExceptionTranslationAutoConfiguration.class,
        TransactionAutoConfiguration.class,
    }
)
public class GenieAgentApplication {
    /**
     * Main method, actual execution is delegated to GenieAgentRunner.
     *
     * @param args command-line arguments
     */
    public static void main(final String[] args) {
        UserConsole.getLogger().info("Starting Genie Agent");
        System.exit(new GenieAgentApplication().run(args));
    }

    private int run(final String[] args) {
        final SpringApplication app = new SpringApplication(GenieAgentApplication.class);
        // Disable parsing of command-line arguments into properties.
        app.setAddCommandLineProperties(false);

        //TODO: workaround for https://jira.spring.io/browse/SPR-17416
        // Spring chokes on argument '--' (a.k.a. bare double dash) conventionally used to separate options from
        // operands. Perform a token replacement to avoid triggering an error in Spring argument parsing.
        // Later the original token is restored before the actual Genie argument parsing.
        final String[] editedArgs = Util.mangleBareDoubleDash(args);
        return SpringApplication.exit(app.run(editedArgs));
    }
}
