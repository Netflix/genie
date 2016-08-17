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
package com.netflix.genie.common.util;

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.text.FieldPosition;
import java.util.Date;

/**
 * Unit tests for the GenieDateFormat class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieDateFormatUnitTests {

    /**
     * Make sure we can properly format a data in UTC time.
     */
    @Test
    public void canFormat() {
        final GenieDateFormat format = new GenieDateFormat();
        final Date zero = new Date(0);
        final StringBuffer buffer = new StringBuffer();
        final FieldPosition pos = Mockito.mock(FieldPosition.class);

        Assert.assertThat(format.format(zero, buffer, pos).toString(), Matchers.is("1970-01-01T00:00:00.000Z"));
    }
}
