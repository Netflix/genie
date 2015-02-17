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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;

/**
 * Test the health check logic.
 *
 * @author tgianos
 */
public class TestHealthCheck {

    private HealthCheck check;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.check = new HealthCheck();
    }

    /**
     * Test the init method.
     */
    @Test
    public void testInit() {
        this.check.init();
    }

    /**
     * Test the getStatus method.
     */
    @Test
    public void testGetStatus() {
        Assert.assertEquals(HttpURLConnection.HTTP_OK, this.check.getStatus());
    }
}
