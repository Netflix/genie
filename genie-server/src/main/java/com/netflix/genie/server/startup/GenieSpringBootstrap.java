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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * This class bootstraps some common Genie spring stuff.
 *
 * @author tgianos
 */
//Using @Component specifically Keeps Karyon/Governator from picking up this bean
@Component
public class GenieSpringBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(GenieSpringBootstrap.class);

    /**
     * Initialize the application here - db connections, daemon threads, etc.
     *
     * @throws Exception
     */
    @PostConstruct
    public void initialize() throws Exception {
        LOG.info("called");
    }

    /**
     * Cleanup/shutdown when context is destroyed.
     */
    @PreDestroy
    public void shutdown() {
        LOG.info("called");

        LOG.info("done");
    }
}
