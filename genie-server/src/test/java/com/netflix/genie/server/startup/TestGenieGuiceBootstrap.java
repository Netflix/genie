/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.google.inject.Module;
import com.netflix.governator.guice.LifecycleInjectorBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for GenieGuiceBootstrap.
 *
 * @author tgianos
 */
public class TestGenieGuiceBootstrap {

    private GenieGuiceBootstrap genieGuiceBootstrap;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.genieGuiceBootstrap = new GenieGuiceBootstrap();
    }

    /**
     * Test the beforeInjectorCreation method.
     */
    @Test
    public void testBeforeInjectorCreation() {
        final LifecycleInjectorBuilder builder = Mockito.mock(LifecycleInjectorBuilder.class);
        this.genieGuiceBootstrap.beforeInjectorCreation(builder);
        Mockito.verify(builder, Mockito.never())
                .withAdditionalModules(Mockito.any(Module.class));
    }
}
