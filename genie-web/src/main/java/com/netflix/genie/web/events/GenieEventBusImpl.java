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
package com.netflix.genie.web.events;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.core.ResolvableType;

import javax.annotation.Nullable;

/**
 * An event bus implementation for the Genie application to use.
 *
 * @author tgianos
 * @since 3.1.2
 */
@Slf4j
public class GenieEventBusImpl implements
    GenieEventBus, ApplicationEventMulticaster, BeanClassLoaderAware, BeanFactoryAware {

    private final SimpleApplicationEventMulticaster syncMulticaster;
    private final SimpleApplicationEventMulticaster asyncMulticaster;

    /**
     * Constructor.
     *
     * @param syncEventMulticaster  The synchronous task multicaster to use
     * @param asyncEventMulticaster The asynchronous task multicaster to use
     */
    public GenieEventBusImpl(
        @NonNull final SimpleApplicationEventMulticaster syncEventMulticaster,
        @NonNull final SimpleApplicationEventMulticaster asyncEventMulticaster
    ) {
        this.syncMulticaster = syncEventMulticaster;
        this.asyncMulticaster = asyncEventMulticaster;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publishSynchronousEvent(@NonNull final ApplicationEvent event) {
        // TODO: Metric here?
        log.debug("Publishing synchronous event {}", event);
        this.syncMulticaster.multicastEvent(event);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publishAsynchronousEvent(@NonNull final ApplicationEvent event) {
        // TODO: Metric here?
        log.debug("Publishing asynchronous event {}", event);
        this.asyncMulticaster.multicastEvent(event);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addApplicationListener(final ApplicationListener<?> listener) {
        log.debug("Adding application listener {}", listener);
        this.syncMulticaster.addApplicationListener(listener);
        this.asyncMulticaster.addApplicationListener(listener);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addApplicationListenerBean(final String listenerBeanName) {
        log.debug("Adding application listener bean with name {}", listenerBeanName);
        this.syncMulticaster.addApplicationListenerBean(listenerBeanName);
        this.asyncMulticaster.addApplicationListenerBean(listenerBeanName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeApplicationListener(final ApplicationListener<?> listener) {
        log.debug("Removing application listener {}", listener);
        this.syncMulticaster.removeApplicationListener(listener);
        this.asyncMulticaster.removeApplicationListener(listener);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeApplicationListenerBean(final String listenerBeanName) {
        log.debug("Removing application listener bean with name {}", listenerBeanName);
        this.syncMulticaster.removeApplicationListenerBean(listenerBeanName);
        this.asyncMulticaster.removeApplicationListenerBean(listenerBeanName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllListeners() {
        log.debug("Removing all application listeners");
        this.syncMulticaster.removeAllListeners();
        this.asyncMulticaster.removeAllListeners();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void multicastEvent(final ApplicationEvent event) {
        // TODO: Metric here?
        log.debug("Multi-casting event {}", event);
        this.asyncMulticaster.multicastEvent(event);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void multicastEvent(final ApplicationEvent event, @Nullable final ResolvableType eventType) {
        // TODO: Metric here?
        log.debug("Multi-casting event {} of type {}", event, eventType);
        this.asyncMulticaster.multicastEvent(event, eventType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setBeanClassLoader(final ClassLoader classLoader) {
        this.syncMulticaster.setBeanClassLoader(classLoader);
        this.asyncMulticaster.setBeanClassLoader(classLoader);
    }

    @Override
    public void setBeanFactory(final BeanFactory beanFactory) {
        this.syncMulticaster.setBeanFactory(beanFactory);
        this.asyncMulticaster.setBeanFactory(beanFactory);
    }
}
