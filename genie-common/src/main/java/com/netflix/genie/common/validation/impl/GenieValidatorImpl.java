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
package com.netflix.genie.common.validation.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.validation.GenieValidator;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * Implementation of GenieValidator interface.
 *
 * @see GenieValidator
 * @author tgianos
 */
public class GenieValidatorImpl implements GenieValidator {

    private static final String NEW_LINE = "\n";

    private final Validator validator;

    /**
     * Construct a new GenieValidatorImpl.
     *
     * @param validator The javax.validator to use.
     */
    public GenieValidatorImpl(final Validator validator) {
        this.validator = validator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> void validate(@NotNull final T bean) throws GenieException {
        final Set<ConstraintViolation<T>> violations = this.validator.validate(bean);
        if (!violations.isEmpty()) {
            final StringBuilder builder = new StringBuilder();
            for (final ConstraintViolation<T> violation : violations) {
                builder.append(violation.getMessage()).append(NEW_LINE);
            }
            throw new GeniePreconditionException(builder.toString());
        }
    }
}
