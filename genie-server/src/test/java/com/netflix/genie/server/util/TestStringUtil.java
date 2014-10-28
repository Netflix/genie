/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.util;

import com.netflix.genie.common.exceptions.GenieException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for StringUtil.
 *
 * @author skrishnan
 * @author tgianos
 */
public class TestStringUtil {

    /**
     * Test various command-line argument permutations to ensure that they parse
     * correctly, including spaces, single and double quotes, etc.
     *
     * @throws GenieException if anything went wrong with the parsing.
     */
    @Test
    public void testSplitCmdLine() throws GenieException {
        final String input = "-f ch_survey_response_f.pig -p vhs_window_start_date=20120912 "
                + "-p survey_date=20120919 -p now_epoch_ts=1348125009 "
                + "-p rap_comment='/**/' -p no_rap_comment='--'";
        final String[] output = StringUtil.splitCmdLine(input);
        Assert.assertTrue(12 == output.length);
    }

    /**
     * Test spaces within quotes.
     *
     * @throws GenieException
     */
    @Test
    public void testSplitCmdLineSpacesWithinQuotes() throws GenieException {
        final String input = "-f pig.q -p endDateTS=\"foo bar\" ";
        final String[] output = StringUtil.splitCmdLine(input);
        Assert.assertTrue(4 == output.length);
    }

    /**
     * Test spaces at beginning and end.
     *
     * @throws GenieException
     */
    @Test
    public void testSplitCmdLineSpacesAtBeginningAndEnd() throws GenieException {
        final String input = " -f pig.q -p bar ";
        final String[] output = StringUtil.splitCmdLine(input);
        Assert.assertTrue(4 == output.length);
    }

    /**
     * Test extra spaces in the middle.
     *
     * @throws GenieException
     */
    @Test
    public void testSplitCmdLineExtraSpacesInMiddle() throws GenieException {
        final String input = "-f device_sum_step1.pig -p from_date=20120915     "
                + "-p to_date=20120918  -p batchid=1347998107";
        final String[] output = StringUtil.splitCmdLine(input);
        Assert.assertTrue(8 == output.length);
    }

    /**
     * Test spaces and equals in middle.
     *
     * @throws GenieException
     */
    @Test
    public void testSplitCmdLineSpacesAndEqualsInMiddle() throws GenieException {
        final String input = "-f hooks.q -d \"setCommandsHook=set hive.exec.reducers.bytes.per.reducer=67108864;\"";
        final String[] output = StringUtil.splitCmdLine(input);
        Assert.assertTrue(4 == output.length);
    }

    /**
     * Test commas in arguments.
     *
     * @throws GenieException
     */
    @Test
    public void testSplitCmdLineCommasInArguments() throws GenieException {
        final String input = "jar searchagg.jar searchevents "
                + "some_agg "
                + "20120916 20120918 "
                + "US,CA,GB "
                + "20 false false";
        final String[] output = StringUtil.splitCmdLine(input);
        Assert.assertTrue(10 == output.length);
    }

    /**
     * Test that null returns empty string.
     *
     * @throws GenieException
     */
    @Test
    public void testSplitCmdLineWithNull() throws GenieException {
        Assert.assertEquals(0, StringUtil.splitCmdLine(null).length);
    }

    /**
     * Test whether Hadoop/Hive/Pig versions are trimmed correctly to 3 digits.
     */
    @Test
    public void testTrim() {
        String input = "0.8.1.3.2";
        String version = StringUtil.trimVersion(input);
        Assert.assertEquals("0.8.1", version);

        input = "0.8.2";
        version = StringUtil.trimVersion(input);
        Assert.assertEquals("0.8.2", version);

        input = "0.8";
        version = StringUtil.trimVersion(input);
        Assert.assertEquals("0.8", version);

        input = "0.8.";
        version = StringUtil.trimVersion(input);
        Assert.assertEquals("0.8", version);

        input = "0";
        version = StringUtil.trimVersion(input);
        Assert.assertEquals("0", version);

        version = StringUtil.trimVersion(null);
        Assert.assertEquals(version, null);
    }

    /**
     * Completeness.
     */
    @Test
    public void testConstructor() {
        final StringUtil util = new StringUtil();
        Assert.assertNotNull(util);
    }
}
