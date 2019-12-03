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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.net.URI;
import java.util.Map;

/**
 * Abstract script class for components that rely on an external script to be loaded and invoked at runtime.
 * This class makes use of {@link ScriptManager} to execute.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public abstract class ManagedScript {
    protected final ObjectMapper mapper;
    private final ScriptManager scriptManager;
    @Getter
    private final ManagedScriptBaseProperties properties;
    private final MeterRegistry registry;

    protected ManagedScript(
        final ScriptManager scriptManager,
        final ManagedScriptBaseProperties properties,
        final ObjectMapper mapper,
        final MeterRegistry registry
    ) {
        this.scriptManager = scriptManager;
        this.properties = properties;
        this.mapper = mapper;
        this.registry = registry;
    }

    /**
     * Request the script to be warm and ready for evaluation.
     * This is a best-effort attempt with no guarantee of success. Furthermore loading may happen asynchronously.
     */
    public void warmUp() {
        final URI scriptUri = this.properties.getSource();
        if (this.properties.isAutoLoadEnabled() && scriptUri != null) {
            this.scriptManager.manageScript(scriptUri);
        }
    }

    protected Object evaluateScript(
        final Map<String, Object> scriptParameters
    ) throws ScriptExecutionException, ScriptNotConfiguredException {

        final URI scriptUri = this.properties.getSource();

        if (scriptUri == null) {
            throw new ScriptNotConfiguredException("Script source URI not set");
        }

        final Bindings bindings = new SimpleBindings();

        for (final Map.Entry<String, Object> parameterEntry : scriptParameters.entrySet()) {
            final String parameterName = parameterEntry.getKey();
            final String parameterValue;
            try {
                parameterValue = this.mapper.writeValueAsString(parameterEntry.getValue());
            } catch (JsonProcessingException e) {
                throw new ScriptExecutionException("Failed to convert parameter: " + parameterName, e);
            }
            bindings.put(parameterName, parameterValue);
        }

        return this.scriptManager.evaluateScript(scriptUri, bindings, this.properties.getTimeout());
    }

    @VisibleForTesting
    boolean isReadyToEvaluate() {
        final URI scriptUri = this.properties.getSource();
        return scriptUri != null && this.scriptManager.isLoaded(scriptUri);
    }
}
