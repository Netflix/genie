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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.exceptions.checked.ScriptLoadingException;
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException;
import com.netflix.genie.web.properties.ScriptManagerProperties;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.TaskScheduler;

import javax.annotation.concurrent.ThreadSafe;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility to load, reload and execute scripts (in whichever format/language supported by {@link ScriptEngine}) via URI
 * (e.g., local file, classpath, URL).
 * <p>
 * N.B.: Scripts must be explicitly registered by calling {@link #manageScript(URI)} in order to be evaluated.
 * <p>
 * N.B.: If a compilation or access error is encountered while reloading a previously compiled script, the latest
 * compiled version is retained, until it can be replaced with a newer one.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@ThreadSafe
public class ScriptManager {
    private static final String SCRIPT_LOAD_TIMER_NAME = "genie.scripts.load.timer";
    private static final String SCRIPT_EVALUATE_TIMER_NAME = "genie.scripts.evaluate.timer";

    private final ConcurrentMap<URI, AtomicReference<CompiledScript>> scriptsMap = Maps.newConcurrentMap();
    private final ScriptManagerProperties properties;
    private final TaskScheduler taskScheduler;
    private final ExecutorService executorService;
    private final ScriptEngineManager scriptEngineManager;
    private final ResourceLoader resourceLoader;
    private final MeterRegistry meterRegistry;

    /**
     * Constructor.
     *
     * @param properties          properties
     * @param taskScheduler       task scheduler
     * @param executorService     executor service
     * @param scriptEngineManager script engine manager
     * @param resourceLoader      resource loader
     * @param meterRegistry       meter registry
     */
    public ScriptManager(
        final ScriptManagerProperties properties,
        final TaskScheduler taskScheduler,
        final ExecutorService executorService,
        final ScriptEngineManager scriptEngineManager,
        final ResourceLoader resourceLoader,
        final MeterRegistry meterRegistry
    ) {
        this.properties = properties;
        this.taskScheduler = taskScheduler;
        this.executorService = executorService;
        this.scriptEngineManager = scriptEngineManager;
        this.resourceLoader = resourceLoader;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Start managing the given script, loading it ASAP (asynchronously) and refreshing it periodically.
     * Because the execution of this task is asynchronous, this method returns immediately and does not surface any
     * loading errors encountered.
     *
     * @param scriptUri the script to load and manage
     */
    public void manageScript(final URI scriptUri) {
        final AtomicBoolean newKey = new AtomicBoolean(false);
        final AtomicReference<CompiledScript> compiledScriptReference = this.scriptsMap.computeIfAbsent(
            scriptUri,
            (key) -> {
                newKey.set(true);
                return new AtomicReference<>();
            }
        );
        if (newKey.get()) {
            this.taskScheduler.scheduleAtFixedRate(
                new LoadScriptTask(scriptUri, compiledScriptReference),
                Instant.now(),
                Duration.ofMillis(this.properties.getRefreshInterval())
            );
            log.debug("Scheduled periodic refresh task for script: {}", scriptUri);
        }
    }

    /**
     * Evaluate a given script.
     *
     * @param scriptUri the script URI
     * @param bindings  the input parameter bindings
     * @param timeout   the timeout in milliseconds
     * @return the result of the evaluation
     * @throws ScriptNotConfiguredException if the script is not loaded (due to invalid URI or compilation errors).
     * @throws ScriptExecutionException     if the script evaluation produces an error
     */
    protected Object evaluateScript(
        final URI scriptUri,
        final Bindings bindings,
        final long timeout
    ) throws ScriptNotConfiguredException, ScriptExecutionException {

        final Set<Tag> tags = Sets.newHashSet();
        tags.add(Tag.of(MetricsConstants.TagKeys.SCRIPT_URI, scriptUri.toString()));

        final long start = System.nanoTime();

        final CompiledScript compiledScript;

        try {
            compiledScript = getCompiledScript(scriptUri);
        } catch (ScriptNotConfiguredException e) {
            final long durationNano = System.nanoTime() - start;
            MetricsUtils.addFailureTagsWithException(tags, e);
            this.meterRegistry.timer(
                SCRIPT_EVALUATE_TIMER_NAME,
                tags
            ).record(durationNano, TimeUnit.NANOSECONDS);
            throw e;
        }

        final Future<Object> taskFuture = executorService.submit(() -> compiledScript.eval(bindings));

        try {
            final Object evaluationResult = taskFuture.get(timeout, TimeUnit.MILLISECONDS);
            MetricsUtils.addSuccessTags(tags);
            return evaluationResult;

        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            // On timeout, stop evaluation. In other cases doesn't hurt
            taskFuture.cancel(true);
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new ScriptExecutionException("Script evaluation failed: " + scriptUri + ": " + e.getMessage(), e);
        } finally {
            final long durationNano = System.nanoTime() - start;
            this.meterRegistry.timer(
                SCRIPT_EVALUATE_TIMER_NAME,
                tags
            ).record(durationNano, TimeUnit.NANOSECONDS);
        }
    }

    private CompiledScript getCompiledScript(final URI scriptUri) throws ScriptNotConfiguredException {
        final AtomicReference<CompiledScript> compiledScriptReference = this.scriptsMap.get(scriptUri);

        if (compiledScriptReference == null) {
            throw new ScriptNotConfiguredException("Unknown script: " + scriptUri);
        }

        final CompiledScript compiledScript = compiledScriptReference.get();

        if (compiledScript == null) {
            throw new ScriptNotConfiguredException("Script not loaded/compiled: " + scriptUri);
        }

        return compiledScript;
    }

    boolean isLoaded(final URI scriptUri) {
        try {
            getCompiledScript(scriptUri);
            return true;
        } catch (ScriptNotConfiguredException e) {
            return false;
        }
    }

    private class LoadScriptTask implements Runnable {
        private final URI scriptUri;
        private final AtomicReference<CompiledScript> compiledScriptReference;

        LoadScriptTask(
            final URI scriptUri,
            final AtomicReference<CompiledScript> compiledScriptReference
        ) {
            this.scriptUri = scriptUri;
            this.compiledScriptReference = compiledScriptReference;
        }

        /**
         * Attempt to load and compile the given script. If successful, stores the resulting {@link CompiledScript} into
         * the provided reference.
         * Also records metrics.
         */
        @Override
        public void run() {

            final Set<Tag> tags = Sets.newHashSet();
            tags.add(Tag.of(MetricsConstants.TagKeys.SCRIPT_URI, scriptUri.toString()));

            final long start = System.nanoTime();

            try {
                final CompiledScript compiledScript = this.loadScript();
                this.compiledScriptReference.set(compiledScript);
                MetricsUtils.addSuccessTags(tags);
            } catch (ScriptLoadingException e) {
                log.error("Failed to load script: " + scriptUri, e);
                MetricsUtils.addFailureTagsWithException(tags, e);
            } catch (Exception e) {
                log.error("Error loading script: " + scriptUri, e);
                MetricsUtils.addFailureTagsWithException(tags, e);
            } finally {
                final long durationNano = System.nanoTime() - start;
                meterRegistry.timer(
                    SCRIPT_LOAD_TIMER_NAME,
                    tags
                ).record(durationNano, TimeUnit.NANOSECONDS);
            }
        }

        private CompiledScript loadScript() throws ScriptLoadingException {

            final String scriptUriString = this.scriptUri.toString();

            // Determine the script type by looking at its filename extension (i.e. .js, .groovy, ...)
            final String scriptExtension = StringUtils.substringAfterLast(this.scriptUri.getPath(), ".");
            if (StringUtils.isBlank(scriptExtension)) {
                throw new ScriptLoadingException("Failed to determine file extension: " + scriptUriString);
            }

            final Resource scriptResource = resourceLoader.getResource(scriptUriString);

            if (!scriptResource.exists()) {
                throw new ScriptLoadingException("Script not found: " + scriptUriString);
            }

            final ScriptEngine engine = scriptEngineManager.getEngineByExtension(scriptExtension);

            if (engine == null) {
                throw new ScriptLoadingException(
                    "Script engine for file extension: " + scriptExtension + " not available");
            }
            if (!(engine instanceof Compilable)) {
                // We want a compilable engine so we can cache the script
                throw new ScriptLoadingException(
                    "Script engine for file extension: " + scriptExtension + " is not " + Compilable.class.getName()
                );
            }

            final Compilable compilable = (Compilable) engine;

            final InputStream scriptInputStream;
            try {
                scriptInputStream = scriptResource.getInputStream();
            } catch (IOException e) {
                throw new ScriptLoadingException("Failed to read script", e);
            }

            final InputStreamReader reader = new InputStreamReader(scriptInputStream, StandardCharsets.UTF_8);
            final CompiledScript compiledScript;
            try {
                compiledScript = compilable.compile(reader);
            } catch (final ScriptException e) {
                throw new ScriptLoadingException("Failed to compile script: " + scriptUriString, e);
            }

            log.info("Successfully compiled: " + scriptUriString);
            return compiledScript;
        }
    }
}
