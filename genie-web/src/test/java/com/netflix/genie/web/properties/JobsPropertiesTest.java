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
package com.netflix.genie.web.properties;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for the JobsProperties class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobsPropertiesTest {

    private JobsMemoryProperties memory;
    private JobsForwardingProperties forwarding;
    private JobsLocationsProperties locations;
    private JobsUsersProperties users;
    private JobsActiveLimitProperties activeLimit;
    private JobsProperties properties;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.memory = Mockito.mock(JobsMemoryProperties.class);
        this.forwarding = Mockito.mock(JobsForwardingProperties.class);
        this.locations = Mockito.mock(JobsLocationsProperties.class);
        this.users = Mockito.mock(JobsUsersProperties.class);
        this.activeLimit = Mockito.mock(JobsActiveLimitProperties.class);
        this.properties = new JobsProperties(
            this.forwarding,
            this.locations,
            this.memory,
            this.users,
            this.activeLimit
        );
    }

    /**
     * Make sure we can construct.
     */
    @Test
    void canConstruct() {
        Assertions.assertThat(this.properties.getMemory()).isNotNull();
        Assertions.assertThat(this.properties.getForwarding()).isNotNull();
        Assertions.assertThat(this.properties.getLocations()).isNotNull();
        Assertions.assertThat(this.properties.getUsers()).isNotNull();
        Assertions.assertThat(this.properties.getActiveLimit()).isNotNull();
    }

    /**
     * Make sure all the setters work.
     */
    @Test
    void canSet() {
        Assertions
            .assertThatCode(
                () -> {
                    this.properties.setForwarding(this.forwarding);
                    this.properties.setLocations(this.locations);
                    this.properties.setMemory(this.memory);
                    this.properties.setUsers(this.users);
                    this.properties.setActiveLimit(this.activeLimit);
                }
            )
            .doesNotThrowAnyException();
    }
}
