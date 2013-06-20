/*
 *
 *  Copyright 2013 Netflix, Inc.
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

import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for StringUtil.
 *
 * @author skrishnan
 */
public class TestStringUtil {

    private String input;
    private String[] output;
    private String version;

    /**
     * Test various command-line argument permutations to ensure that they parse
     * correctly, including spaces, single and double quotes, etc.
     *
     * @throws Exception if anything went wrong with the parsing.
     */
    @Test
    public void testSplitCmdLine() throws Exception {
        System.out.println("Testing for single quotes");
        input = "-f ch_survey_response_f.pig -p vhs_window_start_date=20120912 "
                + "-p survey_date=20120919 -p now_epoch_ts=1348125009 "
                + "-p rap_comment='/**/' -p no_rap_comment='--'";
        output = StringUtil.splitCmdLine(input);
        Assert.assertEquals(output.length, 12);

        System.out.println("Test for spaces within quotes");
        input = "-f pig.q -p endDateTS=\"foo bar\" ";
        output = StringUtil.splitCmdLine(input);
        Assert.assertEquals(output.length, 4);

        System.out.println("Test for spaces at the beginning and end");
        input = " -f pig.q -p bar ";
        output = StringUtil.splitCmdLine(input);
        Assert.assertEquals(output.length, 4);

        System.out.println("Test for extra spaces in the middle");
        input = "-f device_sum_step1.pig -p from_date=20120915     -p to_date=20120918  -p batchid=1347998107";
        output = StringUtil.splitCmdLine(input);
        Assert.assertEquals(output.length, 8);

        System.out.println("Test for spaces and equals in the middle");
        input = "-f hooks.q -d \"setCommandsHook=set hive.exec.reducers.bytes.per.reducer=67108864;\"";
        output = StringUtil.splitCmdLine(input);
        Assert.assertEquals(output.length, 4);

        System.out.println("Testing for commas in arguments");
        input = "jar buzzsearchagg.jar searchevents "
                + "search2click_agg "
                + "20120916 20120918 "
                + "US,CA,GB,GG,IM,JE,IE,SV,BB,BO,SX,MF,NI,AG,CW,AR,JM,MX,AN,DM,LC,GF,PY,VE,BR "
                + "20 false false";
        output = StringUtil.splitCmdLine(input);
        Assert.assertEquals(output.length, 10);

        System.out.println("Testing for quoted strings in arguments");
        input = "jar buzzsearchagg.jar \"foo bar\" \"\"";
        output = StringUtil.splitCmdLine(input);
        Assert.assertEquals(output.length, 4);
    }

    /**
     * Test whether Hadoop/Hive/Pig versions are trimmed correctly to 3 digits.
     */
    @Test
    public void testTrim() {
        input = "0.8.1.3.2";
        System.out.println("Testing for version " + input);
        version = StringUtil.trimVersion(input);
        Assert.assertEquals(version, "0.8.1");

        input = "0.8.2";
        System.out.println("Testing for version " + input);
        version = StringUtil.trimVersion(input);
        Assert.assertEquals(version, "0.8.2");

        input = "0.8";
        System.out.println("Testing for version " + input);
        version = StringUtil.trimVersion(input);
        Assert.assertEquals(version, "0.8");

        input = "0.8.";
        System.out.println("Testing for version " + input);
        version = StringUtil.trimVersion(input);
        Assert.assertEquals(version, "0.8");

        input = "0";
        System.out.println("Testing for version " + input);
        version = StringUtil.trimVersion(input);
        Assert.assertEquals(version, "0");

        input = null;
        System.out.println("Testing for version " + input);
        version = StringUtil.trimVersion(input);
        Assert.assertEquals(version, null);
    }
}
