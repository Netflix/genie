/*
 *
 *  Copyright 2013 Netflix, Inc.
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

package com.netflix.genie.server.util;

import javax.ws.rs.ext.ContextResolver;
import javax.xml.bind.JAXBContext;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;

/**
 * Abstract class that all individual providers extend.
 *
 * @author skrishnan
 */
public abstract class JAXBContextResolver implements ContextResolver<JAXBContext> {

    /**
     * The context that will be initialized using the specified types.
     */
    protected JAXBContext context;

    /**
     * The types that this context resolver supports.
     */
    @SuppressWarnings("rawtypes")
    protected Class[] types;

    /**
     * Constructor - initializes the context based on the given types.
     * This resolver is currently configured to use the "mapped" JSONConfiguration.
     *
     * @param types the types for which this resolver should be used
     * @throws Exception if anything goes wrong during initialization
     */
    @SuppressWarnings("rawtypes")
    public JAXBContextResolver(Class[] types) throws Exception {
        if (types == null) {
            this.types = null;
        } else {
            this.types = new Class[types.length];
            System.arraycopy(types, 0, this.types, 0, types.length);
        }

        this.context = new JSONJAXBContext(
                JSONConfiguration.mapped().
                build(),
                this.types);
    }

    /**
     * Get the context for an object type, if this resolver supports it.
     * Else return null.
     *
     * @return the context, if supported - else null
     */
    @Override
    @SuppressWarnings("rawtypes")
    public JAXBContext getContext(Class<?> objectType) {
        for (Class type : types) {
            if (type == objectType) {
                return context;
            }
        }

        return null;
    }
}
