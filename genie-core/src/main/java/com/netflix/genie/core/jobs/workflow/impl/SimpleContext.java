package com.netflix.genie.core.jobs.workflow.impl;

import com.netflix.genie.core.jobs.workflow.Context;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * Implementation of the Context interface.
 *
 * @author amsharma
 */
public class SimpleContext implements Context {

    private Map<String, Object> context;

    /**
     * Constructor.
     *
     * @param contextInfo context Information for initializing this object
     */
    public SimpleContext(
        @NotNull
        final Map<String, Object> contextInfo
    ) {
        this.context = contextInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAttribute(
        @NotBlank
        final String name,
        @NotNull
        final Object value
    ) {
        context.put(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getAttribute(
        @NotBlank
        final String name
    ) {
        return context.get(name);
    }
}
