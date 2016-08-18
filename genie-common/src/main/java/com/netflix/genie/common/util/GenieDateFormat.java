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

import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.databind.util.ISO8601Utils;

import java.text.FieldPosition;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * An extension of the ISO8601DateFormat to include milliseconds.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class GenieDateFormat extends ISO8601DateFormat {

    /**
     * Constructor.
     */
    public GenieDateFormat() {
        super();
        this.calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StringBuffer format(final Date date, final StringBuffer toAppendTo, final FieldPosition fieldPosition) {
        final String value = ISO8601Utils.format(date, true);
        toAppendTo.append(value);
        return toAppendTo;
    }
}
