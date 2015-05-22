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
import org.junit.BeforeClass;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

/**
 * Test the GenieValidatorImpl class.
 */
public class TestGenieValidatorImpl {

    private static GenieValidatorImpl genieValidator;

    /**
     * Set up the hibernate validator.
     */
    @BeforeClass
    public static void setUpClass() {
        final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        final Validator validator = factory.getValidator();
        genieValidator = new GenieValidatorImpl(validator);
    }

    /**
     * Test to make sure a good bean doesn't throw exceptions.
     *
     * @throws GenieException
     */
    @Test
    public void testGoodBean() throws GenieException {
        final TestValidationBean bean = new TestValidationBean("a", 1, "tom@email.com");
        genieValidator.validate(bean);
    }

    /**
     * Test to make sure a bad bean throws exception.
     *
     * @throws GenieException
     */
    @Test(expected = GeniePreconditionException.class)
    public void testBadBean() throws GenieException {
        final TestValidationBean bean = new TestValidationBean("", -1, "tom.email.com");
        genieValidator.validate(bean);
    }
}
