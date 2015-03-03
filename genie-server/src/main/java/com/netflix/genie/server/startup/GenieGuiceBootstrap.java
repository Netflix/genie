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

import com.netflix.governator.guice.LifecycleInjectorBuilder;
import com.netflix.karyon.server.ServerBootstrap;

/**
 * Bootstraps the Genie application.
 *
 * @author tgianos
 */
public class GenieGuiceBootstrap extends ServerBootstrap {

    /**
     * Add custom Genie bindings to the Karyon bootstrap process.
     *
     * @param builderToBeUsed The builder
     */
    @Override
    protected void beforeInjectorCreation(final LifecycleInjectorBuilder builderToBeUsed) {
//        builderToBeUsed.withAdditionalModules(new GenieModule());
    }
}
