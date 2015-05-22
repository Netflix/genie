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

import org.hibernate.validator.constraints.Email;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

/**
 * A simple java bean to use with the validation tests.
 *
 * @author tgianos
 */
public class TestValidationBean {
    @NotEmpty(message = "a can't be empty.")
    private String a;

    @Min(value = 0, message = "b must be greater than 0.")
    private int b;

    @Email(message = "c must be a valid email address.")
    private String c;

    /**
     * Constructor.
     *
     * @param a not empty
     * @param b greater than 0
     * @param c e-mail
     */
    public TestValidationBean(final String a, final int b, final String c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }
}
