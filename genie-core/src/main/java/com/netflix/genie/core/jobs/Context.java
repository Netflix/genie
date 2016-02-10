package com.netflix.genie.core.jobs;

/**
 * @author amsharma
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
