/*
 *
 *  Copyright 2021 Netflix, Inc.
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
package com.netflix.genie.common.internal.tracing;

/**
 * An interface for implementations to adapt any tags published by default OSS components for internal conventions.
 *
 * @param <U> The type of object which tags should be applied to specific to the implementation
 * @param <K> The key type for the tags
 * @param <V> The value type for the tags
 * @author tgianos
 * @since 4.0.0
 */
public interface TagAdapter<U, K, V> {

    /**
     * The method that should be implemented in order to provide tagging. Genie OSS components should call this method
     * instead of directly tagging.
     *
     * @param taggable The instance which tags should be applied to
     * @param key      The original key for the tag
     * @param value    The original value for the tag
     */
    void tag(U taggable, K key, V value);
}
