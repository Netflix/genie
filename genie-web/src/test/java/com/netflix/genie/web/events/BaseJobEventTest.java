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
package com.netflix.genie.web.events;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Unit tests for BaseJobEvent class.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class BaseJobEventTest {

    /**
     * Make sure constructor saves variables properly.
     */
    @Test
    public void canConstruct() {
        final String id = UUID.randomUUID().toString();
        final Object source = new Object();

        final BaseJobEvent event = new BaseJobEvent(id, source);
        Assert.assertThat(event.getId(), Matchers.is(id));
        Assert.assertThat(event.getSource(), Matchers.is(source));
    }
}
