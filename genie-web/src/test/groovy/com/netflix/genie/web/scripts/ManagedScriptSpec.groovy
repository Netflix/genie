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

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.ImmutableMap
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException
import io.micrometer.core.instrument.MeterRegistry
import spock.lang.Specification

import javax.script.Bindings

class ManagedScriptSpec extends Specification {
    ScriptManager scriptManager
    ObjectMapper objectMapper
    MeterRegistry registry
    TestProperties properties
    TestScript script

    void setup() {
        this.scriptManager = Mock(ScriptManager)
        this.objectMapper = Mock(ObjectMapper)
        this.registry = Mock(MeterRegistry)
        this.properties = new TestProperties()
        this.script = new TestScript(
            scriptManager,
            properties,
            objectMapper,
            registry
        )
    }

    def "LoadPostConstruct"() {
        setup:
        URI uri = new URI("file:///myscript.js")

        when:
        this.script.loadPostConstruct()

        then:
        0 * this.scriptManager.manageScript(_)

        when:
        this.properties.setSource(uri)
        this.script.loadPostConstruct()

        then:
        1 * this.scriptManager.manageScript(uri)
    }


    def "EvaluateScript"() {
        def param1 = new ArrayList()
        def param2 = new Object()
        Map<String, Object> parameters = ImmutableMap.of(
            "x", param1,
            "y", param2
        )
        def result = new Double(123.456)
        URI uri = new URI("file:///foo.js")

        when: "Attempt execution without script configured"
        this.script.evaluateScript(parameters)

        then:
        thrown(ScriptNotConfiguredException)

        when: "Execute script without issues"
        this.properties.setSource(uri)
        def returned = this.script.evaluateScript(parameters)

        then:
        1 * objectMapper.writeValueAsString(param1) >> "{}"
        1 * objectMapper.writeValueAsString(param2) >> "{}"
        1 * scriptManager.evaluateScript(uri, { (it as Bindings).size() == 2 }, {
            it == this.properties.getTimeout()
        }) >> result
        returned == result

        when: "JSON serialization error"
        this.script.evaluateScript(parameters)

        then:
        1 * objectMapper.writeValueAsString(param1) >> "{}"
        1 * objectMapper.writeValueAsString(param2) >> { throw new JsonProcessingException("...") }
        ScriptExecutionException e = thrown(ScriptExecutionException)
        e.getMessage().contains("Failed to convert parameter: y")
        e.getCause().getClass() == JsonProcessingException.class
    }

    private class TestScript extends ManagedScript {
        TestScript(
            final ScriptManager scriptManager,
            final TestProperties properties,
            final ObjectMapper mapper,
            final MeterRegistry registry
        ) {
            super(scriptManager, properties, mapper, registry)
        }
    }

    private class TestProperties extends ManagedScriptBaseProperties {
    }
}
