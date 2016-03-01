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
package com.netflix.genie.core.jpa.entities;

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

/**
 * Unit tests for the SetupFileEntity class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class SetupFileEntityUnitTests extends EntityTestsBase {

    private SetupFileEntity entity;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.entity = new SetupFileEntity();
    }

    /**
     * Make sure getting and setting the setup file field works properly.
     */
    @Test
    public void canSetSetupFile() {
        Assert.assertNull(this.entity.getSetupFile());
        final String setupFile = UUID.randomUUID().toString();
        this.entity.setSetupFile(setupFile);
        Assert.assertThat(this.entity.getSetupFile(), Matchers.is(setupFile));
    }
}
