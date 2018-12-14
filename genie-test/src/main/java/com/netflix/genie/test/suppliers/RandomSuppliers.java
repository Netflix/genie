/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.test.suppliers;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Supply random types.
 *
 * @author tgianos
 * @since 3.0.0
 */
public final class RandomSuppliers {

    /**
     * Get a random String.
     */
    public static final Supplier<String> STRING = UUID.randomUUID()::toString;
    private static final Random RANDOM = new Random();
    /**
     * Get a random integer.
     */
    public static final Supplier<Integer> INT = () -> RANDOM.nextInt(Integer.MAX_VALUE - 1);
    /**
     * Get a random long.
     */
    public static final Supplier<Long> LONG = RANDOM::nextLong;
    /**
     * Get a random instant.
     */
    public static final Supplier<Instant> INSTANT = () -> Instant.ofEpochMilli(LONG.get());

    /**
     * Utility class.
     */
    private RandomSuppliers() {
    }
}
