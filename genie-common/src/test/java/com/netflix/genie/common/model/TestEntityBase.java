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
package com.netflix.genie.common.model;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.validation.GenieValidator;
import com.netflix.genie.common.validation.impl.GenieValidatorImpl;
import org.junit.BeforeClass;

import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

/**
 * Base class for all test classes for entities in the model package.
 *
 * @author tgianos
 */
public class TestEntityBase {

    private static GenieValidator genieValidator;

    /**
     * Setup the validator.
     */
    @BeforeClass
    public static void setupClass() {
        final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        final Validator validator = factory.getValidator();
        genieValidator = new GenieValidatorImpl(validator);
    }

    /**
     * Get the validator object.
     *
     * @param entity The entity to validate
     * @throws GenieException When validation fails
     */
    public <E> void validate(final E entity) throws GenieException {
        genieValidator.validate(entity);
    }
}
