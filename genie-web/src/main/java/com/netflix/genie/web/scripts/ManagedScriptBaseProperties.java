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
package com.netflix.genie.web.scripts;

import lombok.Getter;
import lombok.Setter;
import org.springframework.validation.annotation.Validated;

import jakarta.annotation.Nullable;
import java.net.URI;
import java.time.Duration;

/**
 * Base abstract properties for individual script classes to extend.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@Setter
@Validated
public abstract class ManagedScriptBaseProperties {
    protected static final String SCRIPTS_PREFIX = "genie.scripts";
    protected static final String SOURCE_PROPERTY_SUFFIX = ".source";
    @Nullable
    private URI source;
    private long timeout = 5_000L;
    private boolean autoLoadEnabled = true;
    private Duration propertiesRefreshInterval = Duration.ofMinutes(5);
}
