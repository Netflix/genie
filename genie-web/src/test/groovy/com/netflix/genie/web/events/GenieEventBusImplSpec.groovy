/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.events


import org.springframework.beans.factory.BeanFactory
import org.springframework.context.ApplicationEvent
import org.springframework.context.ApplicationListener
import org.springframework.context.event.SimpleApplicationEventMulticaster
import org.springframework.core.ResolvableType
import spock.lang.Specification

/**
 * Specification for the GenieEventBusImpl.
 *
 * @author tgianos
 */
class GenieEventBusImplSpec extends Specification {

    def syncMulticaster = Mock(SimpleApplicationEventMulticaster)
    def asyncMulticaster = Mock(SimpleApplicationEventMulticaster)
    def eventBus = new GenieEventBusImpl(this.syncMulticaster, this.asyncMulticaster)

    def "Can publish synchronous event"() {
        def event = Mock(ApplicationEvent)

        when:
        this.eventBus.publishSynchronousEvent(event)

        then:
        1 * this.syncMulticaster.multicastEvent(event)
        0 * this.asyncMulticaster.multicastEvent(event)
    }

    def "Can publish asynchronous event"() {
        def event = Mock(ApplicationEvent)

        when:
        this.eventBus.publishAsynchronousEvent(event)

        then:
        0 * this.syncMulticaster.multicastEvent(event)
        1 * this.asyncMulticaster.multicastEvent(event)
    }

    def "Can add application listener"() {
        def listener = Mock(ApplicationListener)

        when:
        this.eventBus.addApplicationListener(listener)

        then:
        1 * this.syncMulticaster.addApplicationListener(listener)
        1 * this.asyncMulticaster.addApplicationListener(listener)
    }

    def "Can add application listener bean"() {
        def beanName = UUID.randomUUID().toString()

        when:
        this.eventBus.addApplicationListenerBean(beanName)

        then:
        1 * this.syncMulticaster.addApplicationListenerBean(beanName)
        1 * this.asyncMulticaster.addApplicationListenerBean(beanName)
    }

    def "Can remove application listener"() {
        def listener = Mock(ApplicationListener)

        when:
        this.eventBus.removeApplicationListener(listener)

        then:
        1 * this.syncMulticaster.removeApplicationListener(listener)
        1 * this.asyncMulticaster.removeApplicationListener(listener)
    }

    def "Can remove application listener bean"() {
        def beanName = UUID.randomUUID().toString()

        when:
        this.eventBus.removeApplicationListenerBean(beanName)

        then:
        1 * this.syncMulticaster.removeApplicationListenerBean(beanName)
        1 * this.asyncMulticaster.removeApplicationListenerBean(beanName)
    }

    def "Can remove all listeners"() {
        when:
        this.eventBus.removeAllListeners()

        then:
        1 * this.syncMulticaster.removeAllListeners()
        1 * this.asyncMulticaster.removeAllListeners()
    }

    def "Can multicast event"() {
        def event = Mock(ApplicationEvent)

        when:
        this.eventBus.multicastEvent(event)

        then:
        0 * this.syncMulticaster.multicastEvent(event)
        1 * this.asyncMulticaster.multicastEvent(event)
    }

    def "Can multicast event with resolvable type"() {
        def event = Mock(ApplicationEvent)
        def type = Mock(ResolvableType)

        when:
        this.eventBus.multicastEvent(event, type)

        then:
        0 * this.syncMulticaster.multicastEvent(event, type)
        1 * this.asyncMulticaster.multicastEvent(event, type)
    }

    def "Can set bean factory"() {
        def beanFactory = Mock(BeanFactory)

        when:
        this.eventBus.setBeanFactory(beanFactory)

        then:
        1 * this.syncMulticaster.setBeanFactory(beanFactory)
        1 * this.asyncMulticaster.setBeanFactory(beanFactory)
    }

    def "Can set bean class loader"() {
        def classLoader = Mock(ClassLoader)

        when:
        this.eventBus.setBeanClassLoader(classLoader)

        then:
        1 * this.syncMulticaster.setBeanClassLoader(classLoader)
        1 * this.asyncMulticaster.setBeanClassLoader(classLoader)
    }
}
