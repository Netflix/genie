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
package com.netflix.genie.web.data.services.impl.jpa.entities;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Base class for all test classes for entities in the model package.
 *
 * @author tgianos
 */
class EntityTestBase {

    private static Validator validator;

    /**
     * Setup the validator.
     */
    @BeforeAll
    static void setupClass() {
        final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    /**
     * Get the validator object.
     *
     * @param <E>    The type of entity to validate
     * @param entity The entity to validate
     */
    <E> void validate(final E entity) {
        final Set<ConstraintViolation<E>> violations = validator.validate(entity);
        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }

    <T> void testOptionalField(
        final Supplier<Optional<T>> getter,
        final Consumer<T> setter,
        final T testValue
    ) {
        Assertions.assertThat(getter.get()).isNotPresent();
        setter.accept(null);
        Assertions.assertThat(getter.get()).isNotPresent();
        setter.accept(testValue);
        Assertions.assertThat(getter.get()).isPresent().contains(testValue);
    }
}
