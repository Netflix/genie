/*
 * Copyright 2015 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */
package com.netflix.genie.server.health;

import com.netflix.karyon.spi.HealthCheckHandler;
import java.net.HttpURLConnection;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom health check for Genie goes here - although it is quite non-custom
 * right now.
 *
 * @author skrishnan
 */
public class HealthCheck implements HealthCheckHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HealthCheck.class);

    /**
     * Any custom initialization goes here - but empty right now.
     */
    @PostConstruct
    public void init() {
        LOG.info("Health check initialized.");
    }

    /**
     * Should return custom status based on actual health - currently it always
     * returns 200 (HTTP_OK).
     *
     * @return HTTP health status
     */
    @Override
    public int getStatus() {
        //TODO: Custom health check logic goes here
        LOG.debug("Health check invoked.");
        return HttpURLConnection.HTTP_OK;
    }
}
