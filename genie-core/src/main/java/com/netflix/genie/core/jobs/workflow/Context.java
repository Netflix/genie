/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.jobs.workflow;

/**
 * @author amsharma
 * @since 3.0.0
 */

/**
 * Interface defining a context.
 *
 * Any class implementing this interface should provide all the information that needs to
 * be shared between different actions in a workflow.
 *
 */
public interface Context {

    /**
     * Set the appropriate object with a name.
     *
     * @param name name that identifies an attribute
     * @param value object that is associated with the name
     */
     void setAttribute(String name, Object value);


    /**
     * Gets the object associated with the  name.
     *
     * @param name A string that identifies the object to be returned.
     * @return Object linked to that name
     */
     Object getAttribute(String name);

}
