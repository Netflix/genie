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
package com.netflix.genie.web.properties;

import com.netflix.genie.web.scripts.CommandSelectorManagedScript;
import com.netflix.genie.web.scripts.ManagedScriptBaseProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties for {@link CommandSelectorManagedScript}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = CommandSelectorManagedScriptProperties.PREFIX)
public class CommandSelectorManagedScriptProperties extends ManagedScriptBaseProperties {
    /**
     * Prefix for this properties class.
     */
    public static final String PREFIX = ManagedScriptBaseProperties.SCRIPTS_PREFIX + ".command-selector";
    /**
     * Name of script source property.
     */
    public static final String SOURCE_PROPERTY = PREFIX + ManagedScriptBaseProperties.SOURCE_PROPERTY_SUFFIX;

    /**
     * Prefix for properties passed to the script (with the prefix stripped).
     */
    public static final String SCRIPT_PROPERTIES_PREFIX = "command-selector.";
}
