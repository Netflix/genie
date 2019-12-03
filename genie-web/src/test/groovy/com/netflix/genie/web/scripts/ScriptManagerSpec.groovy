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
package com.netflix.genie.web.scripts

import com.netflix.genie.web.exceptions.checked.ScriptExecutionException
import com.netflix.genie.web.exceptions.checked.ScriptLoadingException
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException
import com.netflix.genie.web.properties.ScriptManagerProperties
import com.netflix.genie.web.util.MetricsConstants
import com.netflix.genie.web.util.MetricsUtils
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import org.springframework.scheduling.TaskScheduler
import spock.lang.Specification

import javax.script.Bindings
import javax.script.Compilable
import javax.script.CompiledScript
import javax.script.ScriptEngine
import javax.script.ScriptEngineManager
import javax.script.ScriptException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

class ScriptManagerSpec extends Specification {
    ScriptManagerProperties scriptManagerProperties
    TaskScheduler taskScheduler
    ExecutorService executorService
    ScriptEngineManager scriptEngineManager
    ResourceLoader resourceLoader
    MeterRegistry meterRegistry
    ScriptManager scriptManager

    void setup() {
        this.scriptManagerProperties = new ScriptManagerProperties()
        this.taskScheduler = Mock(TaskScheduler)
        this.executorService = Mock(ExecutorService)
        this.scriptEngineManager = Mock(ScriptEngineManager)
        this.resourceLoader = Mock(ResourceLoader)
        this.meterRegistry = Mock(MeterRegistry)

        this.scriptManager = new ScriptManager(
            scriptManagerProperties,
            taskScheduler,
            executorService,
            scriptEngineManager,
            resourceLoader,
            meterRegistry
        )
    }

    def "manageScript"() {
        URI script1 = new URI("file:///myscript.js")
        URI script2 = new URI("s3:///some-bucket/scripts/myscript.js")

        when: "Register duplicate script..."
        this.scriptManager.manageScript(script1)
        this.scriptManager.manageScript(script1)

        then: "...does not schedule duplicate refresh"

        when: "Register different script..."
        this.scriptManager.manageScript(script2)

        then: "...schedules refresh"
        1 * this.taskScheduler.scheduleAtFixedRate(_ as Runnable, _ as Instant, _ as Duration)
    }

    def "LoadScriptTask"() {
        URI scriptUri = new URI("file:///myscript.js")

        AtomicReference compiledScriptReference
        ScriptManager.LoadScriptTask loadScriptTask

        Timer timer = Mock(Timer)
        Set<Tag> loadingErrorTags = MetricsUtils.newFailureTagsSetForException(new ScriptLoadingException())
        Set<Tag> loadingRuntimeErrorTags = MetricsUtils.newFailureTagsSetForException(new RuntimeException())
        Set<Tag> loadingSuccessTags = MetricsUtils.newSuccessTagsSet()

        Tag uriTag = Tag.of(MetricsConstants.TagKeys.SCRIPT_URI, scriptUri.toString())
        loadingErrorTags.add(uriTag)
        loadingSuccessTags.add(uriTag)

        Resource scriptResource = Mock(Resource)
        ScriptEngine engine = Mock(CompilableScriptEngine)
        InputStream inputStream = Mock(InputStream)
        CompiledScript compiledScript = Mock(CompiledScript)
        CompiledScript recompiledScript = Mock(CompiledScript)

        when: "Register script..."
        this.scriptManager.manageScript(scriptUri)
        compiledScriptReference = this.scriptManager.scriptsMap.get(scriptUri)

        then: "...capture loading task"
        1 * this.taskScheduler.scheduleAtFixedRate(_ as Runnable, _ as Instant, _ as Duration) >> {
            args ->
                loadScriptTask = args[0] as ScriptManager.LoadScriptTask
                return Mock(ScheduledFuture)
        }
        compiledScriptReference != null
        loadScriptTask != null
        scriptUri == loadScriptTask.scriptUri
        compiledScriptReference == loadScriptTask.compiledScriptReference

        when: "Loading succeeds"
        loadScriptTask.run()

        then:
        1 * resourceLoader.getResource(scriptUri.toString()) >> scriptResource
        1 * scriptResource.exists() >> true
        1 * scriptEngineManager.getEngineByExtension("js") >> engine
        1 * scriptResource.getInputStream() >> inputStream
        1 * engine.compile(_ as Reader) >> compiledScript
        1 * meterRegistry.timer(
            ScriptManager.SCRIPT_LOAD_TIMER_NAME,
            { Set<Tag> tags -> tags.containsAll(loadingSuccessTags) }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        compiledScript == compiledScriptReference.get()

        when: "Error loading resource"
        loadScriptTask.run()

        then:
        1 * resourceLoader.getResource(scriptUri.toString()) >> scriptResource
        1 * scriptResource.exists() >> { throw new RuntimeException("...") }
        1 * meterRegistry.timer(
            ScriptManager.SCRIPT_LOAD_TIMER_NAME,
            { Set<Tag> tags -> tags.containsAll(loadingRuntimeErrorTags) }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        compiledScript == compiledScriptReference.get()

        when: "Resource does not exist"
        loadScriptTask.run()

        then:
        1 * resourceLoader.getResource(scriptUri.toString()) >> scriptResource
        1 * scriptResource.exists() >> false
        1 * meterRegistry.timer(
            ScriptManager.SCRIPT_LOAD_TIMER_NAME,
            { Set<Tag> tags -> tags.containsAll(loadingErrorTags) }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        compiledScript == compiledScriptReference.get()

        when: "Engine does not exist for extension"
        loadScriptTask.run()

        then:
        1 * resourceLoader.getResource(scriptUri.toString()) >> scriptResource
        1 * scriptResource.exists() >> true
        1 * scriptEngineManager.getEngineByExtension("js") >> null
        1 * meterRegistry.timer(
            ScriptManager.SCRIPT_LOAD_TIMER_NAME,
            { Set<Tag> tags -> tags.containsAll(loadingErrorTags) }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        compiledScript == compiledScriptReference.get()

        when: "Engine is not compilable"
        loadScriptTask.run()

        then:
        1 * resourceLoader.getResource(scriptUri.toString()) >> scriptResource
        1 * scriptResource.exists() >> true
        1 * scriptEngineManager.getEngineByExtension("js") >> Mock(InterpretedScriptEngine)
        1 * meterRegistry.timer(
            ScriptManager.SCRIPT_LOAD_TIMER_NAME,
            { Set<Tag> tags -> tags.containsAll(loadingErrorTags) }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        compiledScript == compiledScriptReference.get()

        when: "Resource is not readable"
        loadScriptTask.run()

        then:
        1 * resourceLoader.getResource(scriptUri.toString()) >> scriptResource
        1 * scriptResource.exists() >> true
        1 * scriptEngineManager.getEngineByExtension("js") >> engine
        1 * scriptResource.getInputStream() >> { throw new IOException("...") }
        1 * meterRegistry.timer(
            ScriptManager.SCRIPT_LOAD_TIMER_NAME,
            { Set<Tag> tags -> tags.containsAll(loadingErrorTags) }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        compiledScript == compiledScriptReference.get()

        when: "Compilation fails"
        loadScriptTask.run()

        then:
        1 * resourceLoader.getResource(scriptUri.toString()) >> scriptResource
        1 * scriptResource.exists() >> true
        1 * scriptEngineManager.getEngineByExtension("js") >> engine
        1 * scriptResource.getInputStream() >> inputStream
        1 * engine.compile(_ as Reader) >> { throw new ScriptException("...") }
        1 * meterRegistry.timer(
            ScriptManager.SCRIPT_LOAD_TIMER_NAME,
            { Set<Tag> tags -> tags.containsAll(loadingErrorTags) }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        compiledScript == compiledScriptReference.get()

        when: "Loading succeeds and replaces previously compiled script"
        loadScriptTask.run()

        then:
        1 * resourceLoader.getResource(scriptUri.toString()) >> scriptResource
        1 * scriptResource.exists() >> true
        1 * scriptEngineManager.getEngineByExtension("js") >> engine
        1 * scriptResource.getInputStream() >> inputStream
        1 * engine.compile(_ as Reader) >> recompiledScript
        1 * meterRegistry.timer(
            ScriptManager.SCRIPT_LOAD_TIMER_NAME,
            { Set<Tag> tags -> tags.containsAll(loadingSuccessTags) }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        recompiledScript == compiledScriptReference.get()
    }

    def "LoadScriptTask -- script with no extension"() {
        URI scriptUri = new URI("file:///myscript")

        AtomicReference compiledScriptReference
        ScriptManager.LoadScriptTask loadScriptTask

        Timer timer = Mock(Timer)
        Set<Tag> loadingErrorTags = MetricsUtils.newFailureTagsSetForException(new ScriptLoadingException())
        Set<Tag> loadingSuccessTags = MetricsUtils.newSuccessTagsSet()

        Tag uriTag = Tag.of(MetricsConstants.TagKeys.SCRIPT_URI, scriptUri.toString())
        loadingErrorTags.add(uriTag)
        loadingSuccessTags.add(uriTag)

        when: "Register script..."
        this.scriptManager.manageScript(scriptUri)
        compiledScriptReference = this.scriptManager.scriptsMap.get(scriptUri)

        then: "...capture loading task"
        1 * this.taskScheduler.scheduleAtFixedRate(_ as Runnable, _ as Instant, _ as Duration) >> {
            args ->
                loadScriptTask = args[0] as ScriptManager.LoadScriptTask
                return Mock(ScheduledFuture)
        }
        compiledScriptReference != null
        loadScriptTask != null
        scriptUri == loadScriptTask.scriptUri
        compiledScriptReference == loadScriptTask.compiledScriptReference

        when: "Attempt to load"
        loadScriptTask.run()

        then:
        0 * resourceLoader.getResource(scriptUri.toString())
        1 * meterRegistry.timer(
            ScriptManager.SCRIPT_LOAD_TIMER_NAME,
            { Set<Tag> tags -> tags.containsAll(loadingErrorTags) }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
    }

    def "evaluateScript"() {
        URI scriptUri = new URI("s3://genie/scripts/myscript.js")
        CompiledScript compiledScript = Mock(CompiledScript)
        Throwable evaluationException = new TimeoutException("...")
        Object evaluationResult = new Object()

        Timer timer = Mock(Timer)
        Set<Tag> evalErrorTags = MetricsUtils.newFailureTagsSetForException(evaluationException)
        Set<Tag> evalSuccessTags = MetricsUtils.newSuccessTagsSet()

        Tag uriTag = Tag.of(MetricsConstants.TagKeys.SCRIPT_URI, scriptUri.toString())
        evalErrorTags.add(uriTag)
        evalSuccessTags.add(uriTag)

        Bindings bindings = Mock(Bindings)
        long timeout = 5_000L
        Future<Object> evalTaskFuture = Mock(Future)

        when: "Evaluate unknown script"
        this.scriptManager.evaluateScript(scriptUri, bindings, timeout)

        then:
        thrown(ScriptNotConfiguredException)

        when: "Evaluate script not yet compiled successfully"
        this.scriptManager.scriptsMap.put(scriptUri, new AtomicReference<>(null))
        this.scriptManager.evaluateScript(scriptUri, bindings, timeout)

        then:
        thrown(ScriptNotConfiguredException)

        when:
        this.scriptManager.scriptsMap.put(scriptUri, new AtomicReference<>(compiledScript))
        this.scriptManager.evaluateScript(scriptUri, bindings, timeout)

        then:
        1 * executorService.submit(_) >> evalTaskFuture
        1 * evalTaskFuture.get(timeout, TimeUnit.MILLISECONDS) >> { throw evaluationException }
        1 * evalTaskFuture.cancel(true)
        1 * meterRegistry.timer(
            ScriptManager.SCRIPT_EVALUATE_TIMER_NAME,
            {
                Set<Tag> tags -> tags.containsAll(evalErrorTags)
            }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        thrown(ScriptExecutionException)

        when:
        def result = this.scriptManager.evaluateScript(scriptUri, bindings, timeout)

        then:
        1 * executorService.submit(_) >> evalTaskFuture
        1 * evalTaskFuture.get(timeout, TimeUnit.MILLISECONDS) >> evaluationResult
        1 * meterRegistry.timer(
            ScriptManager.SCRIPT_EVALUATE_TIMER_NAME,
            {
                Set<Tag> tags -> tags.containsAll(evalSuccessTags)
            }
        ) >> timer
        1 * timer.record({ it > 0 }, TimeUnit.NANOSECONDS)
        result == evaluationResult
    }
//
//    def "Load script with no extension"() {
//        URI scriptUri = new URI("file:///foo")
//        Resource scriptResource = Mock(Resource)
//
//        Runnable loadScriptRunnable
//
//        when: "Force load to capture loading task"
//        this.scriptManager.manageScript(scriptUri)
//
//        then:
//        1 * asyncTaskExecutor.submit(_ as Runnable) >> {
//            args ->
//                loadScriptRunnable = args[0] as Runnable
//                return Mock(Future)
//        }
//        loadScriptRunnable != null
//        noExceptionThrown()
//
//        when: "Script filename extension is null or empty"
//        loadScriptRunnable.run()
//
//        then:
//        1 * resourceLoader.getResource(scriptUri.toString()) >> scriptResource
//        1 * scriptResource.exists() >> true
//        0 * scriptEngineManager.getEngineByExtension(_)
//        noExceptionThrown()
//    }
//
//    @Unroll
//    def "Script evaluation exception: #exception"() {
//        CompiledScript compiledScript = Mock(CompiledScript)
//        Bindings bindings = Mock(Bindings)
//        long timeout = 5000L
//        Future<Object> evaluateTaskFuture = Mock(Future)
//        Callable<Object> evaluateTask
//
//        setup:
//        URI scriptUri = new URI("file:///foo")
//        this.scriptManager.scriptsMap.put(scriptUri, new AtomicReference<CompiledScript>(compiledScript))
//
//        when:
//        this.scriptManager.evaluateScript(scriptUri, bindings, timeout)
//
//        then:
//        1 * asyncTaskExecutor.submit(_ as Callable) >> {
//            args ->
//                evaluateTask = args[0] as Callable<Object>
//                return evaluateTaskFuture
//        }
//        1 * evaluateTaskFuture.get(timeout, TimeUnit.MILLISECONDS) >> { throw exception }
//        if (exception.getClass() == TimeoutException) {
//            1 * evaluateTaskFuture.cancel(true)
//        } else {
//            0 * evaluateTaskFuture.cancel(true)
//        }
//        Throwable e = thrown(ScriptExecutionException)
//        exception == e.getCause()
//
//        where:
//        _ | exception
//        _ | new TimeoutException("...")
//        _ | new InterruptedException("...")
//        _ | new ExecutionException("...")
//    }
//
//    def "Successful evaluation"() {
//        CompiledScript compiledScript = Mock(CompiledScript)
//        Bindings bindings = Mock(Bindings)
//        long timeout = 5000L
//        Future<Object> evaluateTaskFuture = Mock(Future)
//        Callable<Object> evaluateTask
//        Object result = "YAY"
//
//        setup:
//        URI scriptUri = new URI("file:///foo")
//        this.scriptManager.scriptsMap.put(scriptUri, new AtomicReference<CompiledScript>(compiledScript))
//
//        when:
//        def actualResult = this.scriptManager.evaluateScript(scriptUri, bindings, timeout)
//
//        then:
//        1 * asyncTaskExecutor.submit(_ as Callable) >> {
//            args ->
//                evaluateTask = args[0] as Callable<Object>
//                return evaluateTaskFuture
//        }
//        1 * evaluateTaskFuture.get(timeout, TimeUnit.MILLISECONDS) >> result
//        result == actualResult
//    }
//
//    def "EvaluateScript"() {
//        URI scriptUri = new URI("file:///foo.js")
//        Bindings bindings = Mock(Bindings)
//        long timeout = 5000L
//        Throwable e
//
//        when: "Execute a script never seen before"
//        this.scriptManager.evaluateScript(scriptUri, bindings, timeout)
//
//        then:
//        e = thrown(ScriptNotConfiguredException)
//        e.getMessage().contains("not loaded")
//        this.scriptManager.scriptsMap.containsKey(scriptUri)
//
//        when: "Execute a script not yet compiled"
//        this.scriptManager.evaluateScript(scriptUri, bindings, timeout)
//
//        then:
//        e = thrown(ScriptNotConfiguredException)
//        e.getMessage().contains("not loaded")
//    }

    abstract class CompilableScriptEngine implements ScriptEngine, Compilable {
    }

    abstract class InterpretedScriptEngine implements ScriptEngine {
    }
}
