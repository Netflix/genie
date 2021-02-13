/*
 *
 *  Copyright 2020 Netflix, Inc.
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

import com.google.common.collect.Maps
import com.google.common.collect.Sets
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.internal.util.PropertiesMapCache
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException
import com.netflix.genie.web.selectors.ResourceSelectionContext
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification

import javax.script.Bindings

class ResourceSelectorScriptSpec extends Specification {

    Map<String, String> cachedProperties = Maps.newHashMap()
    URI scriptUri = URI.create("s3://scripts/resource-selector.groovy")
    TestResource testResource1 = new TestResource()
    TestResource testResource2 = new TestResource()
    Set<TestResource> testResources = Sets.newHashSet(testResource1, testResource2)
    ResourceSelectorScriptResult<TestResource> selectResource1Result =
        new ResourceSelectorScriptResult.Builder()
            .withResource(testResource1)
            .withRationale("Because!")
            .build()
    ResourceSelectorScriptResult<TestResource> selectOtherResourceResult =
        new ResourceSelectorScriptResult.Builder()
            .withResource(new TestResource())
            .withRationale("Because!")
            .build()

    ScriptManager scriptManager
    TestProperties scriptProperties
    MeterRegistry registry
    PropertiesMapCache cache
    TestResourceSelectorScript resourceSelectorScript
    TestSelectionContext context

    void setup() {
        this.scriptManager = Mock(ScriptManager)
        this.scriptProperties = new TestProperties()
        this.registry = new SimpleMeterRegistry()
        this.cache = Mock(PropertiesMapCache)
        this.context = Mock(TestSelectionContext)

        this.resourceSelectorScript = new TestResourceSelectorScript(
            scriptManager,
            scriptProperties,
            registry,
            cache
        )
    }

    def "Successfully select resource"() {
        this.scriptProperties.setSource(scriptUri)
        cachedProperties.put("foo", "bar")
        ResourceSelectorScriptResult<TestResource> selected

        when:
        selected = resourceSelectorScript.selectResource(context)

        then:
        1 * cache.get() >> cachedProperties
        1 * scriptManager.evaluateScript(_ as URI, _ as Bindings, _) >> {
            URI u, Bindings b, long t ->
                assert b.get(ResourceSelectorScript.PROPERTIES_MAP_BINDING) == cachedProperties
                assert b.get(ResourceSelectorScript.CONTEXT_BINDING) == context
                return selectResource1Result
        }
        1 * context.getResources() >> testResources
        selected != null
        selected.getResource().get() == testResource1
    }

    def "Successfully select no resource"() {
        this.scriptProperties.setSource(scriptUri)
        ResourceSelectorScriptResult<TestResource> selected

        ResourceSelectorScriptResult<TestResource> emptySelection = new ResourceSelectorScriptResult.Builder().build()

        when:
        selected = resourceSelectorScript.selectResource(context)

        then:
        1 * cache.get() >> cachedProperties
        1 * scriptManager.evaluateScript(_, _, _) >> emptySelection
        selected != null
        !selected.getResource().isPresent()
    }

    def "Handle errors"() {
        this.scriptProperties.setSource(scriptUri)
        def runtimeException = new RuntimeException("...")

        when:
        resourceSelectorScript.selectResource(context)

        then:
        1 * cache.get() >> { throw runtimeException }
        def e = thrown(ResourceSelectionException)
        e.getCause() == runtimeException

        when:
        resourceSelectorScript.selectResource(context)

        then:
        1 * cache.get() >> cachedProperties
        1 * scriptManager.evaluateScript(_, _, _) >> new Object()
        thrown(ResourceSelectionException)

        when:
        resourceSelectorScript.selectResource(context)

        then:
        1 * cache.get() >> cachedProperties
        1 * scriptManager.evaluateScript(_, _, _) >> selectResource1Result
        1 * context.getResources() >> Sets.newHashSet()
        thrown(ResourceSelectionException)

        when:

        resourceSelectorScript.selectResource(context)

        then:
        1 * cache.get() >> cachedProperties
        1 * scriptManager.evaluateScript(_, _, _) >> selectOtherResourceResult
        1 * context.getResources() >> testResources
        thrown(ResourceSelectionException)

        when:
        this.scriptProperties.setSource(null)
        resourceSelectorScript.selectResource(context)

        then:
        1 * cache.get() >> cachedProperties
        thrown(ResourceSelectionException)
    }

    class TestResourceSelectorScript extends ResourceSelectorScript<TestResource, TestSelectionContext> {
        protected TestResourceSelectorScript(
            final ScriptManager scriptManager,
            final ManagedScriptBaseProperties properties,
            final MeterRegistry registry,
            final PropertiesMapCache dynamicPropertyMapCache
        ) {
            super(scriptManager, properties, registry, dynamicPropertyMapCache)
        }
    }

    static class TestResource {
        TestResource() {}
    }

    abstract class TestSelectionContext extends ResourceSelectionContext<TestResource> {
        TestSelectionContext(final String jobId, final JobRequest jobRequest, final boolean apiJob) {
            super(jobId, jobRequest, apiJob)
        }
    }

    class TestProperties extends ManagedScriptBaseProperties {
    }
}
